package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"

	"github.com/example/sync-vector-engine/internal/crdt"
	"github.com/example/sync-vector-engine/internal/storage"
	"github.com/example/sync-vector-engine/internal/types"
)

const (
	defaultInterval          = 15 * time.Second
	defaultWALThreshold      = int64(500)
	defaultMutationThreshold = 256
)

// Payload captures the CRDT state and metadata persisted inside an object
// storage snapshot.
type Payload struct {
	Document    types.DocumentID     `json:"document_id"`
	LastOpID    types.OperationID    `json:"last_op_id"`
	VectorClock types.VectorClock    `json:"vector_clock"`
	Nodes       []crdt.CharacterNode `json:"nodes"`
}

// Worker periodically inspects per-document mutation volume and emits CRDT
// snapshots to object storage when thresholds are exceeded.
type Worker struct {
	wal    *storage.WAL
	engine *crdt.Engine
	object *minio.Client
	bucket string

	interval          time.Duration
	walThreshold      int64
	mutationThreshold int

	logger zerolog.Logger
}

// NewWorker constructs a snapshot worker with sane defaults.
func NewWorker(wal *storage.WAL, engine *crdt.Engine, object *minio.Client, bucket string, logger zerolog.Logger) *Worker {
	return &Worker{
		wal:               wal,
		engine:            engine,
		object:            object,
		bucket:            bucket,
		interval:          defaultInterval,
		walThreshold:      defaultWALThreshold,
		mutationThreshold: defaultMutationThreshold,
		logger:            logger,
	}
}

// Start begins the periodic snapshot loop.
func (w *Worker) Start(ctx context.Context) {
	go w.loop(ctx)
}

func (w *Worker) loop(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.runOnce(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) runOnce(ctx context.Context) {
	for _, docID := range w.engine.Documents() {
		if err := w.processDocument(ctx, docID); err != nil {
			w.logger.Error().Err(err).Str("document", string(docID)).Msg("snapshot emission failed")
		}
	}
}

func (w *Worker) processDocument(ctx context.Context, docID types.DocumentID) error {
	if w.object == nil {
		return fmt.Errorf("object storage client not configured")
	}

	latest, err := w.wal.LatestSnapshot(ctx, docID)
	if err != nil {
		return fmt.Errorf("lookup latest snapshot: %w", err)
	}

	walCount, err := w.wal.OperationCountAfterLSN(ctx, docID, latest.LastLSN)
	if err != nil {
		return fmt.Errorf("count operations: %w", err)
	}

	nodeCount := w.engine.Store(docID).NodeCount(true)
	if walCount < w.walThreshold && nodeCount < w.mutationThreshold {
		return nil
	}

	lastOp := w.engine.LastOperation(docID)
	if lastOp == "" {
		return nil
	}

	payload := Payload{
		Document:    docID,
		LastOpID:    lastOp,
		VectorClock: w.engine.VectorClock(docID),
		Nodes:       w.engine.Store(docID).Snapshot(true),
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode snapshot payload: %w", err)
	}

	objectPath := fmt.Sprintf("snapshots/%s/%s.bin", docID, lastOp)
	if _, err := w.object.PutObject(ctx, w.bucket, objectPath, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: "application/octet-stream"}); err != nil {
		return fmt.Errorf("upload snapshot: %w", err)
	}

	ref := storage.SnapshotRef{
		Document:    docID,
		OperationID: lastOp,
		VectorClock: payload.VectorClock.Clone(),
		ObjectPath:  objectPath,
		LastLSN:     w.engine.LastLSN(docID),
		CreatedAt:   time.Now().UTC(),
	}

	if err := w.wal.RecordSnapshot(ctx, ref); err != nil {
		return fmt.Errorf("persist snapshot ref: %w", err)
	}

	w.logger.Info().Str("document", string(docID)).Str("op_id", string(lastOp)).Msg("snapshot created")
	return nil
}

// DecodePayload unmarshals a snapshot payload from its binary representation.
func DecodePayload(data []byte) (Payload, error) {
	var payload Payload
	if err := json.Unmarshal(data, &payload); err != nil {
		return Payload{}, err
	}
	return payload, nil
}
