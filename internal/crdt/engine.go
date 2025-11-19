package crdt

import (
	"encoding/json"
	"sync"

	"github.com/rs/zerolog"

	"github.com/example/sync-vector-engine/internal/types"
)

// Engine orchestrates CRDT stores per document and tracks applied WAL positions.
type Engine struct {
	mu      sync.RWMutex
	siteID  string
	stores  map[types.DocumentID]*CRDTStore
	clocks  map[types.DocumentID]types.VectorClock
	lastLSN map[types.DocumentID]int64
	logger  zerolog.Logger
}

// NewEngine constructs an Engine with the provided site identifier and logger.
func NewEngine(siteID string, logger zerolog.Logger) *Engine {
	return &Engine{
		siteID:  siteID,
		stores:  make(map[types.DocumentID]*CRDTStore),
		clocks:  make(map[types.DocumentID]types.VectorClock),
		lastLSN: make(map[types.DocumentID]int64),
		logger:  logger,
	}
}

// Store returns the CRDTStore for a document, creating it if necessary.
func (e *Engine) Store(docID types.DocumentID) *CRDTStore {
	e.mu.Lock()
	defer e.mu.Unlock()

	store, ok := e.stores[docID]
	if ok {
		return store
	}

	store = NewCRDTStore(e.siteID)
	e.stores[docID] = store
	return store
}

// ApplyWAL replays a WAL record into the CRDT store and merges vector clocks.
func (e *Engine) ApplyWAL(record types.WALRecord) error {
	store := e.Store(record.Document)
	e.mu.Lock()
	clock := e.clocks[record.Document]
	if clock == nil {
		clock = make(types.VectorClock)
	}
	clock.Merge(record.VectorClock)
	e.clocks[record.Document] = clock
	e.lastLSN[record.Document] = record.LSN
	e.mu.Unlock()

	if len(record.Payload) == 0 {
		return nil
	}

	var evt Event
	if err := json.Unmarshal(record.Payload, &evt); err != nil {
		e.logger.Error().Err(err).Str("document", string(record.Document)).Msg("failed to decode WAL payload")
		return err
	}

	switch evt.Type {
	case EventInsert:
		store.ApplyInsert(evt.Node.OriginLeft, evt.Node.OriginRight, evt.Node.Content)
	case EventDelete:
		store.ApplyDelete(evt.Node.ID)
	}

	return nil
}

// VectorClock returns the current logical clock for a document.
func (e *Engine) VectorClock(docID types.DocumentID) types.VectorClock {
	e.mu.RLock()
	defer e.mu.RUnlock()

	clock := e.clocks[docID]
	if clock == nil {
		return make(types.VectorClock)
	}
	copy := make(types.VectorClock)
	for k, v := range clock {
		copy[k] = v
	}
	return copy
}

// LastLSN returns the highest applied WAL position for the document.
func (e *Engine) LastLSN(docID types.DocumentID) int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastLSN[docID]
}

// Documents returns the list of documents currently loaded in memory.
func (e *Engine) Documents() []types.DocumentID {
	e.mu.RLock()
	defer e.mu.RUnlock()

	docs := make([]types.DocumentID, 0, len(e.stores))
	for docID := range e.stores {
		docs = append(docs, docID)
	}
	return docs
}
