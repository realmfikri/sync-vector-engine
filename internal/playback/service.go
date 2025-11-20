package playback

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/rs/zerolog"

	"github.com/example/sync-vector-engine/internal/crdt"
	"github.com/example/sync-vector-engine/internal/snapshot"
	"github.com/example/sync-vector-engine/internal/storage"
	"github.com/example/sync-vector-engine/internal/types"
)

var errPlaybackComplete = errors.New("playback complete")

// Log provides the read operations required to hydrate a document at a specific
// point in time.
type Log interface {
	LSNForOperation(ctx context.Context, docID types.DocumentID, opID types.OperationID) (int64, time.Time, error)
	LSNForTime(ctx context.Context, docID types.DocumentID, ts time.Time) (int64, error)
	SnapshotBeforeLSN(ctx context.Context, docID types.DocumentID, lsn int64) (storage.SnapshotRef, error)
	ReplayDocument(ctx context.Context, docID types.DocumentID, fromLSN int64, handler func(types.WALRecord) error) error
}

// SnapshotLoader fetches binary snapshot payloads from object storage.
type SnapshotLoader interface {
	Load(ctx context.Context, bucket, objectPath string) ([]byte, error)
}

// Authorizer validates that a caller can access a particular document.
type Authorizer interface {
	Authorize(ctx context.Context, docID types.DocumentID) error
}

// AllowAllAuthorizer is a no-op authorizer used when callers have already been validated upstream.
type AllowAllAuthorizer struct{}

// Authorize implements Authorizer.
func (AllowAllAuthorizer) Authorize(context.Context, types.DocumentID) error { return nil }

// Request captures the playback cursor for a document.
type Request struct {
	Document    types.DocumentID
	OperationID types.OperationID
	AtTime      *time.Time
}

// Response is the hydrated document buffer and causality metadata.
type Response struct {
	Document    types.DocumentID  `json:"document_id"`
	OperationID types.OperationID `json:"operation_id"`
	LSN         int64             `json:"lsn"`
	VectorClock types.VectorClock `json:"vector_clock"`
	Buffer      []byte            `json:"buffer"`
}

// Service replays snapshots and WAL deltas to surface deterministic document
// state at a requested logical point.
type Service struct {
	wal    Log
	bucket string
	loader SnapshotLoader
	auth   Authorizer
	cache  *stateCache
	logger zerolog.Logger
	siteID string
}

// ServiceConfig configures optional behaviours for playback.
type ServiceConfig struct {
	Authorizer Authorizer
	CacheSize  int
	SiteID     string
}

// NewService constructs a playback service backed by the provided WAL reader
// and object storage loader.
func NewService(wal Log, bucket string, loader SnapshotLoader, logger zerolog.Logger, cfg ServiceConfig) *Service {
	cacheSize := cfg.CacheSize
	if cacheSize == 0 {
		cacheSize = 8
	}
	siteID := cfg.SiteID
	if siteID == "" {
		siteID = "playback"
	}

	return &Service{
		wal:    wal,
		bucket: bucket,
		loader: loader,
		auth:   cfg.Authorizer,
		cache:  newStateCache(cacheSize),
		logger: logger,
		siteID: siteID,
	}
}

// Playback hydrates the document at the requested operation or timestamp.
func (s *Service) Playback(ctx context.Context, req Request) (Response, error) {
	if req.Document == "" {
		return Response{}, errors.New("document id is required")
	}
	if req.OperationID == "" && req.AtTime == nil {
		return Response{}, errors.New("at_op or at_time is required")
	}
	if s.auth != nil {
		if err := s.auth.Authorize(ctx, req.Document); err != nil {
			return Response{}, fmt.Errorf("access denied: %w", err)
		}
	}

	targetLSN, targetOp, targetClock, err := s.resolveTarget(ctx, req)
	if err != nil {
		return Response{}, err
	}

	// Fast path: reuse a cached state that already satisfies the target LSN.
	if cached, ok := s.cache.Get(req.Document, targetLSN); ok {
		store := crdt.NewCRDTStore(s.siteID)
		store.Reset(cached.Nodes)
		engine := crdt.NewEngine(s.siteID, s.logger)
		engine.Restore(req.Document, cached.Nodes, cached.VectorClock.Clone(), cached.LastOp, cached.LSN)
		return s.replayFrom(ctx, engine, req.Document, cached.LSN, targetLSN, targetOp, targetClock)
	}

	snapshotState, err := s.fetchSnapshot(ctx, req.Document, targetLSN)
	if err != nil {
		return Response{}, err
	}

	engine := crdt.NewEngine(s.siteID, s.logger)
	engine.Restore(req.Document, snapshotState.Nodes, snapshotState.Vector.Clone(), snapshotState.LastOp, snapshotState.LSN)

	return s.replayFrom(ctx, engine, req.Document, snapshotState.LSN, targetLSN, targetOp, targetClock)
}

func (s *Service) replayFrom(ctx context.Context, engine *crdt.Engine, docID types.DocumentID, fromLSN, targetLSN int64, targetOp types.OperationID, targetClock types.VectorClock) (Response, error) {
	appliedOp := engine.LastOperation(docID)
	appliedClock := engine.VectorClock(docID)

	if fromLSN >= targetLSN {
		buffer := []byte(engine.Store(docID).ToText())
		vector := appliedClock.Clone()
		return Response{Document: docID, OperationID: appliedOp, LSN: targetLSN, VectorClock: vector, Buffer: buffer}, nil
	}

	err := s.wal.ReplayDocument(ctx, docID, fromLSN, func(record types.WALRecord) error {
		if record.LSN > targetLSN {
			return errPlaybackComplete
		}
		appliedOp = record.Operation
		appliedClock.Merge(record.VectorClock)
		return engine.ApplyWAL(record)
	})
	if err != nil && !errors.Is(err, errPlaybackComplete) {
		return Response{}, fmt.Errorf("replay document: %w", err)
	}

	buffer := []byte(engine.Store(docID).ToText())
	vector := appliedClock.Clone()

	s.cache.Put(docID, cacheEntry{
		LSN:         targetLSN,
		LastOp:      appliedOp,
		VectorClock: vector.Clone(),
		Nodes:       engine.Store(docID).Snapshot(true),
	})

	respOp := targetOp
	if respOp == "" {
		respOp = appliedOp
	}
	if targetClock != nil {
		vector.Merge(targetClock)
	}

	return Response{
		Document:    docID,
		OperationID: respOp,
		LSN:         targetLSN,
		VectorClock: vector,
		Buffer:      buffer,
	}, nil
}

type snapshotState struct {
	Nodes  []crdt.CharacterNode
	Vector types.VectorClock
	LSN    int64
	LastOp types.OperationID
}

func (s *Service) fetchSnapshot(ctx context.Context, docID types.DocumentID, targetLSN int64) (snapshotState, error) {
	ref, err := s.wal.SnapshotBeforeLSN(ctx, docID, targetLSN)
	if err != nil {
		return snapshotState{}, fmt.Errorf("find snapshot: %w", err)
	}
	if ref.ObjectPath == "" {
		return snapshotState{Vector: make(types.VectorClock)}, nil
	}

	payloadBytes, err := s.loader.Load(ctx, s.bucket, ref.ObjectPath)
	if err != nil {
		return snapshotState{}, fmt.Errorf("load snapshot object: %w", err)
	}

	payload, err := snapshot.DecodePayload(payloadBytes)
	if err != nil {
		return snapshotState{}, fmt.Errorf("decode snapshot: %w", err)
	}

	return snapshotState{
		Nodes:  payload.Nodes,
		Vector: payload.VectorClock.Clone(),
		LSN:    ref.LastLSN,
		LastOp: payload.LastOpID,
	}, nil
}

func (s *Service) resolveTarget(ctx context.Context, req Request) (int64, types.OperationID, types.VectorClock, error) {
	if req.OperationID != "" {
		lsn, createdAt, err := s.wal.LSNForOperation(ctx, req.Document, req.OperationID)
		if err != nil {
			return 0, "", nil, fmt.Errorf("lookup operation: %w", err)
		}
		if req.AtTime != nil && req.AtTime.Before(createdAt) {
			return 0, "", nil, fmt.Errorf("requested time predates operation %s", req.OperationID)
		}
		return lsn, req.OperationID, nil, nil
	}

	lsn, err := s.wal.LSNForTime(ctx, req.Document, *req.AtTime)
	if err != nil {
		return 0, "", nil, fmt.Errorf("lookup lsn for time: %w", err)
	}
	return lsn, "", make(types.VectorClock), nil
}

// ObjectLoader fetches raw bytes from object storage.
type ObjectLoader struct {
	object *minio.Client
}

// NewObjectLoader creates a loader backed by MinIO/S3.
func NewObjectLoader(object *minio.Client) *ObjectLoader {
	return &ObjectLoader{object: object}
}

// Load implements SnapshotLoader.
func (l *ObjectLoader) Load(ctx context.Context, bucket, objectPath string) ([]byte, error) {
	if l.object == nil {
		return nil, errors.New("object storage client is not configured")
	}

	obj, err := l.object.GetObject(ctx, bucket, objectPath, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// MemoryLoader is a helper used in tests to return embedded snapshots.
type MemoryLoader struct {
	Objects map[string][]byte
}

// Load implements SnapshotLoader.
func (m MemoryLoader) Load(_ context.Context, _, objectPath string) ([]byte, error) {
	data, ok := m.Objects[objectPath]
	if !ok {
		return nil, fmt.Errorf("object %s not found", objectPath)
	}
	return data, nil
}
