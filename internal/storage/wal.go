package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/sync-vector-engine/internal/types"
)

// WAL provides persistence for CRDT operations and recovery helpers.
type WAL struct {
	pool       *pgxpool.Pool
	maxRetries int
	retryDelay time.Duration
}

// WALOption configures the WAL store.
type WALOption func(*WAL)

// WithMaxRetries sets the maximum retry count for transient failures.
func WithMaxRetries(n int) WALOption {
	return func(w *WAL) {
		w.maxRetries = n
	}
}

// WithRetryDelay sets the base delay between retries.
func WithRetryDelay(d time.Duration) WALOption {
	return func(w *WAL) {
		w.retryDelay = d
	}
}

// NewWAL constructs a WAL helper using the provided Postgres pool.
func NewWAL(pool *pgxpool.Pool, opts ...WALOption) *WAL {
	w := &WAL{
		pool:       pool,
		maxRetries: 3,
		retryDelay: 100 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// AppendOperation durably stores an operation for the provided document.
// The insert is wrapped in a transaction and transient failures are retried.
func (w *WAL) AppendOperation(ctx context.Context, docID types.DocumentID, op types.WALRecord) (int64, error) {
	op.Document = docID
	if op.CreatedAt.IsZero() {
		op.CreatedAt = time.Now().UTC()
	}

	var lsn int64
	err := w.retry(ctx, func(ctx context.Context) error {
		tx, err := w.pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		vectorBytes, err := json.Marshal(op.VectorClock)
		if err != nil {
			return fmt.Errorf("marshal vector clock: %w", err)
		}

		row := tx.QueryRow(ctx, `
INSERT INTO document_operations (document_id, op_id, client_id, vector_clock, payload, created_at)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING lsn`,
			op.Document, op.Operation, op.Client, vectorBytes, op.Payload, op.CreatedAt,
		)
		if err := row.Scan(&lsn); err != nil {
			return err
		}

		if err := tx.Commit(ctx); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	return lsn, nil
}

// AppendCRDTOperation converts a logical operation into a WAL record and
// persists it, ensuring the vector clock is encoded alongside the payload.
func (w *WAL) AppendCRDTOperation(ctx context.Context, op types.Operation) (int64, error) {
	record := op.ToWALRecord()
	return w.AppendOperation(ctx, record.Document, record)
}

// ActiveDocuments returns the set of documents that currently have WAL entries.
func (w *WAL) ActiveDocuments(ctx context.Context) ([]types.DocumentID, error) {
	rows, err := w.pool.Query(ctx, `SELECT DISTINCT document_id FROM document_operations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var docs []types.DocumentID
	for rows.Next() {
		var doc string
		if err := rows.Scan(&doc); err != nil {
			return nil, err
		}
		docs = append(docs, types.DocumentID(doc))
	}
	return docs, rows.Err()
}

// ReplayDocument scans operations for a document ordered by op_id, invoking the handler for each record.
func (w *WAL) ReplayDocument(ctx context.Context, docID types.DocumentID, fromLSN int64, handler func(types.WALRecord) error) error {
	rows, err := w.pool.Query(ctx, `
                SELECT lsn, document_id, op_id, client_id, vector_clock, payload, created_at
                FROM document_operations
                WHERE document_id = $1 AND lsn > $2
                ORDER BY op_id`, docID, fromLSN)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			lsn         int64
			documentID  string
			opID        string
			clientID    string
			vectorClock []byte
			payload     []byte
			createdAt   time.Time
		)
		if err := rows.Scan(&lsn, &documentID, &opID, &clientID, &vectorClock, &payload, &createdAt); err != nil {
			return err
		}

		var clock types.VectorClock
		if len(vectorClock) > 0 {
			if err := json.Unmarshal(vectorClock, &clock); err != nil {
				return fmt.Errorf("decode vector clock: %w", err)
			}
		}

		record := types.WALRecord{
			LSN:         lsn,
			Operation:   types.OperationID(opID),
			Document:    types.DocumentID(documentID),
			Client:      types.ClientID(clientID),
			Payload:     payload,
			VectorClock: clock,
			CreatedAt:   createdAt,
		}

		if err := handler(record); err != nil {
			return err
		}
	}

	return rows.Err()
}

// LastCheckpoint returns the most recent persisted LSN for a document.
func (w *WAL) LastCheckpoint(ctx context.Context, docID types.DocumentID) (int64, error) {
	var lsn int64
	err := w.pool.QueryRow(ctx, `
                SELECT last_lsn FROM document_checkpoints WHERE document_id = $1
        `, docID).Scan(&lsn)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return lsn, err
}

// RecordCheckpoint upserts the current LSN for a document.
func (w *WAL) RecordCheckpoint(ctx context.Context, docID types.DocumentID, lsn int64) error {
	return w.retry(ctx, func(ctx context.Context) error {
		_, err := w.pool.Exec(ctx, `
                        INSERT INTO document_checkpoints (document_id, last_lsn)
                        VALUES ($1, $2)
                        ON CONFLICT (document_id)
                        DO UPDATE SET last_lsn = EXCLUDED.last_lsn, checkpointed_at = now()
                `, docID, lsn)
		return err
	})
}

func (w *WAL) retry(ctx context.Context, fn func(context.Context) error) error {
	delay := w.retryDelay
	for attempt := 0; attempt <= w.maxRetries; attempt++ {
		if err := fn(ctx); err != nil {
			if !isTransient(err) || attempt == w.maxRetries {
				return err
			}
			select {
			case <-time.After(delay):
				delay *= 2
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		return nil
	}
	return nil
}

func isTransient(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", // serialization_failure
			"40P01": // deadlock_detected
			return true
		}
	}

	var connectErr *pgconn.ConnectError
	return errors.As(err, &connectErr)
}
