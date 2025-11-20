package playback

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/example/sync-vector-engine/internal/crdt"
	"github.com/example/sync-vector-engine/internal/snapshot"
	"github.com/example/sync-vector-engine/internal/storage"
	"github.com/example/sync-vector-engine/internal/types"
)

type fakeLog struct {
	ops       []types.WALRecord
	snapshots map[int64]storage.SnapshotRef
	replays   int
}

func (f *fakeLog) LSNForOperation(_ context.Context, docID types.DocumentID, opID types.OperationID) (int64, time.Time, error) {
	for _, op := range f.ops {
		if op.Document == docID && op.Operation == opID {
			return op.LSN, op.CreatedAt, nil
		}
	}
	return 0, time.Time{}, errors.New("operation not found")
}

func (f *fakeLog) LSNForTime(_ context.Context, docID types.DocumentID, ts time.Time) (int64, error) {
	var lsn int64
	for _, op := range f.ops {
		if op.Document != docID || op.CreatedAt.After(ts) {
			continue
		}
		if op.LSN > lsn {
			lsn = op.LSN
		}
	}
	return lsn, nil
}

func (f *fakeLog) SnapshotBeforeLSN(_ context.Context, docID types.DocumentID, lsn int64) (storage.SnapshotRef, error) {
	var best storage.SnapshotRef
	for _, ref := range f.snapshots {
		if ref.Document != docID || ref.LastLSN > lsn {
			continue
		}
		if ref.LastLSN > best.LastLSN {
			best = ref
		}
	}
	return best, nil
}

func (f *fakeLog) ReplayDocument(_ context.Context, docID types.DocumentID, fromLSN int64, handler func(types.WALRecord) error) error {
	f.replays++
	for _, op := range f.ops {
		if op.Document != docID || op.LSN <= fromLSN {
			continue
		}
		if err := handler(op); err != nil {
			return err
		}
	}
	return nil
}

func TestPlaybackDeterministicForOverlappingTimes(t *testing.T) {
	docID := types.DocumentID("doc-1")
	base := time.Now()

	log := &fakeLog{
		ops: []types.WALRecord{
			walInsert(1, "op-1", docID, "alice", base, crdt.Identifier{Path: []int{1}, SiteID: "alice"}, "H"),
			walInsert(2, "op-2", docID, "bob", base.Add(1*time.Minute), crdt.Identifier{Path: []int{2}, SiteID: "bob"}, "i"),
			walDelete(3, "op-3", docID, "alice", base.Add(2*time.Minute), crdt.Identifier{Path: []int{1}, SiteID: "alice"}),
		},
		snapshots: map[int64]storage.SnapshotRef{},
	}

	svc := NewService(log, "", MemoryLoader{}, zeroLogger(), ServiceConfig{CacheSize: 4})

	early := base.Add(90 * time.Second)  // after op-2 but before op-3
	later := base.Add(150 * time.Second) // after delete op-3

	resp1, err := svc.Playback(context.Background(), Request{Document: docID, AtTime: &early})
	if err != nil {
		t.Fatalf("playback1 err: %v", err)
	}

	resp2, err := svc.Playback(context.Background(), Request{Document: docID, AtTime: &later})
	if err != nil {
		t.Fatalf("playback2 err: %v", err)
	}

	if string(resp1.Buffer) != "Hi" {
		t.Fatalf("expected 'Hi', got %q", string(resp1.Buffer))
	}
	if string(resp2.Buffer) != "i" {
		t.Fatalf("expected 'i', got %q", string(resp2.Buffer))
	}

	if log.replays > 2 {
		t.Fatalf("expected at most 2 replays, got %d", log.replays)
	}
}

func TestPlaybackUsesSnapshotsAndCache(t *testing.T) {
	docID := types.DocumentID("doc-2")
	base := time.Now()

	snapPayload := snapshot.Payload{
		Document:    docID,
		LastOpID:    types.OperationID("op-snap"),
		VectorClock: types.VectorClock{"alice": 1},
		Nodes: []crdt.CharacterNode{{
			ID:         crdt.Identifier{Path: []int{1}, SiteID: "alice"},
			Content:    "O",
			OriginLeft: crdt.Identifier{},
		}},
	}
	snapBytes, err := json.Marshal(snapPayload)
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}

	log := &fakeLog{
		ops: []types.WALRecord{
			walInsert(3, "op-4", docID, "bob", base.Add(30*time.Second), crdt.Identifier{Path: []int{2}, SiteID: "bob"}, "K"),
		},
		snapshots: map[int64]storage.SnapshotRef{
			2: {Document: docID, OperationID: "op-snap", ObjectPath: "snap.bin", LastLSN: 2},
		},
	}

	loader := MemoryLoader{Objects: map[string][]byte{"snap.bin": snapBytes}}
	svc := NewService(log, "bucket", loader, zeroLogger(), ServiceConfig{CacheSize: 2})

	resp, err := svc.Playback(context.Background(), Request{Document: docID, OperationID: "op-4"})
	if err != nil {
		t.Fatalf("playback err: %v", err)
	}

	if string(resp.Buffer) != "OK" {
		t.Fatalf("expected 'OK', got %q", string(resp.Buffer))
	}

	// Replaying the same target should leverage the cache and avoid another full WAL scan.
	_, err = svc.Playback(context.Background(), Request{Document: docID, OperationID: "op-4"})
	if err != nil {
		t.Fatalf("second playback err: %v", err)
	}

	if log.replays > 2 {
		t.Fatalf("expected cache to cap replays, got %d", log.replays)
	}
}

func walInsert(lsn int64, op string, docID types.DocumentID, client string, ts time.Time, id crdt.Identifier, content string) types.WALRecord {
	evt := crdt.Event{Type: crdt.EventInsert, Node: crdt.CharacterNode{ID: id, OriginLeft: crdt.Identifier{}, OriginRight: crdt.Identifier{}, Content: content}}
	payload, _ := json.Marshal(evt)
	return types.WALRecord{LSN: lsn, Operation: types.OperationID(op), Document: docID, Client: types.ClientID(client), Payload: payload, VectorClock: types.VectorClock{types.ClientID(client): uint64(lsn)}, CreatedAt: ts}
}

func walDelete(lsn int64, op string, docID types.DocumentID, client string, ts time.Time, id crdt.Identifier) types.WALRecord {
	evt := crdt.Event{Type: crdt.EventDelete, Node: crdt.CharacterNode{ID: id}}
	payload, _ := json.Marshal(evt)
	return types.WALRecord{LSN: lsn, Operation: types.OperationID(op), Document: docID, Client: types.ClientID(client), Payload: payload, VectorClock: types.VectorClock{types.ClientID(client): uint64(lsn)}, CreatedAt: ts}
}

func zeroLogger() zerolog.Logger {
	return zerolog.New(io.Discard)
}
