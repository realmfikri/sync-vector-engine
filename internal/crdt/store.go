package crdt

import (
	"sort"
	"sync"
)

// EventType enumerates CRDT lifecycle transitions.
type EventType string

const (
	EventInsert EventType = "insert"
	EventDelete EventType = "delete"
)

// Event describes a change to the CRDT structure that subscribers can persist
// or broadcast to peers.
type Event struct {
	Type EventType
	Node CharacterNode
}

// Listener receives CRDT events.
type Listener func(Event)

// CRDTStore keeps an ordered collection of CharacterNodes and publishes events
// on state changes.
type CRDTStore struct {
	mu        sync.RWMutex
	nodes     []*CharacterNode
	generator *IdentifierGenerator
	listeners []Listener
}

// NewCRDTStore constructs a CRDT store configured for the given site.
func NewCRDTStore(siteID string) *CRDTStore {
	return &CRDTStore{generator: NewIdentifierGenerator(siteID)}
}

// Subscribe registers a listener to receive CRDT events. It returns a function
// to unregister the listener when it is no longer needed.
func (s *CRDTStore) Subscribe(listener Listener) func() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.listeners = append(s.listeners, listener)
	idx := len(s.listeners) - 1
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if idx >= 0 && idx < len(s.listeners) {
			s.listeners = append(s.listeners[:idx], s.listeners[idx+1:]...)
		}
	}
}

func (s *CRDTStore) emit(evt Event) {
	listeners := s.listenersSnapshot()
	for _, listener := range listeners {
		listener(evt)
	}
}

// ApplyInsert generates an identifier between the provided origins and inserts
// a new character node into the index while preserving ordering.
func (s *CRDTStore) ApplyInsert(originLeft, originRight Identifier, content string) CharacterNode {
	s.mu.Lock()

	id := s.generator.Generate(originLeft, originRight)
	node := CharacterNode{
		ID:          id,
		OriginLeft:  originLeft,
		OriginRight: originRight,
		Content:     content,
	}

	idx := sort.Search(len(s.nodes), func(i int) bool {
		return s.nodes[i].ID.Compare(id) >= 0
	})

	s.nodes = append(s.nodes, nil)
	copy(s.nodes[idx+1:], s.nodes[idx:])
	s.nodes[idx] = &node

	s.mu.Unlock()

	s.emit(Event{Type: EventInsert, Node: node})
	return node
}

// ApplyDelete marks the node with the provided identifier as deleted, retaining
// the tombstone for ordering and playback.
func (s *CRDTStore) ApplyDelete(id Identifier) (CharacterNode, bool) {
	s.mu.Lock()

	idx := sort.Search(len(s.nodes), func(i int) bool {
		return s.nodes[i].ID.Compare(id) >= 0
	})
	if idx >= len(s.nodes) || !s.nodes[idx].ID.Equals(id) {
		s.mu.Unlock()
		return CharacterNode{}, false
	}

	s.nodes[idx].IsDeleted = true
	node := *s.nodes[idx]

	s.mu.Unlock()

	s.emit(Event{Type: EventDelete, Node: node})
	return node, true
}

// Snapshot returns the current state of the CRDT nodes. When includeDeleted is
// false, tombstoned nodes are omitted from the result.
func (s *CRDTStore) Snapshot(includeDeleted bool) []CharacterNode {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]CharacterNode, 0, len(s.nodes))
	for _, n := range s.nodes {
		if !includeDeleted && n.IsDeleted {
			continue
		}
		result = append(result, *n)
	}
	return result
}

// ToText flattens the CRDT structure into a linear document string, skipping
// deleted entries.
func (s *CRDTStore) ToText() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	buf := make([]byte, 0, len(s.nodes))
	for _, n := range s.nodes {
		if n.IsDeleted {
			continue
		}
		buf = append(buf, n.Content...)
	}
	return string(buf)
}

// Iterator returns a channel that yields nodes in order. When skipDeleted is
// true tombstoned nodes are not emitted.
func (s *CRDTStore) Iterator(skipDeleted bool) <-chan CharacterNode {
	out := make(chan CharacterNode)
	go func() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		defer close(out)

		for _, n := range s.nodes {
			if skipDeleted && n.IsDeleted {
				continue
			}
			out <- *n
		}
	}()
	return out
}

func (s *CRDTStore) listenersSnapshot() []Listener {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return append([]Listener(nil), s.listeners...)
}
