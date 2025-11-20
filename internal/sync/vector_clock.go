package syncstate

import (
	"sync"

	"github.com/example/sync-vector-engine/internal/types"
)

// VectorClockTracker maintains per-document logical clocks. Local operations
// should call Bump before emission while remote operations invoke Merge to
// fold in their clock state.
type VectorClockTracker struct {
	mu    sync.RWMutex
	clock map[types.DocumentID]types.VectorClock
}

// NewVectorClockTracker constructs an empty tracker.
func NewVectorClockTracker() *VectorClockTracker {
	return &VectorClockTracker{
		clock: make(map[types.DocumentID]types.VectorClock),
	}
}

// BumpLocal increments the logical clock for the provided client/document pair
// and returns the updated clock snapshot suitable for attaching to a new
// outbound operation.
func (t *VectorClockTracker) BumpLocal(docID types.DocumentID, client types.ClientID) types.VectorClock {
	t.mu.Lock()
	defer t.mu.Unlock()

	clock := t.ensure(docID)
	clock.Bump(client)

	return clock.Clone()
}

// MergeRemote merges a remote vector clock into the document clock and returns
// the updated snapshot.
func (t *VectorClockTracker) MergeRemote(docID types.DocumentID, other types.VectorClock) types.VectorClock {
	t.mu.Lock()
	defer t.mu.Unlock()

	clock := t.ensure(docID)
	clock.Merge(other)
	t.clock[docID] = clock

	return clock.Clone()
}

// Snapshot returns a copy of the current logical clock for the document.
func (t *VectorClockTracker) Snapshot(docID types.DocumentID) types.VectorClock {
	t.mu.RLock()
	defer t.mu.RUnlock()

	clock := t.clock[docID]
	if clock == nil {
		return make(types.VectorClock)
	}
	return clock.Clone()
}

// Dominates reports whether the current clock for the document covers the
// provided vector clock, meaning all counters are greater or equal.
func (t *VectorClockTracker) Dominates(docID types.DocumentID, other types.VectorClock) bool {
	t.mu.RLock()
	clock := t.clock[docID]
	t.mu.RUnlock()

	if clock == nil {
		return len(other) == 0
	}
	return clock.Dominates(other)
}

func (t *VectorClockTracker) ensure(docID types.DocumentID) types.VectorClock {
	clock := t.clock[docID]
	if clock == nil {
		clock = make(types.VectorClock)
		t.clock[docID] = clock
	}
	return clock
}
