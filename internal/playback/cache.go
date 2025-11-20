package playback

import (
	"container/list"
	"sync"

	"github.com/example/sync-vector-engine/internal/crdt"
	"github.com/example/sync-vector-engine/internal/types"
)

type cacheKey struct {
	Document types.DocumentID
	LSN      int64
}

// cacheEntry stores a reusable CRDT snapshot for a particular WAL position.
type cacheEntry struct {
	LSN         int64
	LastOp      types.OperationID
	VectorClock types.VectorClock
	Nodes       []crdt.CharacterNode
}

type stateCache struct {
	mu       sync.Mutex
	capacity int
	ll       *list.List
	items    map[cacheKey]*list.Element
}

func newStateCache(capacity int) *stateCache {
	if capacity < 1 {
		capacity = 1
	}
	return &stateCache{
		capacity: capacity,
		ll:       list.New(),
		items:    make(map[cacheKey]*list.Element),
	}
}

func (c *stateCache) Get(docID types.DocumentID, targetLSN int64) (cacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var bestKey cacheKey
	var bestItem *list.Element

	for key, item := range c.items {
		if key.Document != docID || key.LSN > targetLSN {
			continue
		}
		if bestItem == nil || key.LSN > bestKey.LSN {
			bestKey = key
			bestItem = item
		}
	}

	if bestItem == nil {
		return cacheEntry{}, false
	}

	c.ll.MoveToFront(bestItem)
	entry := bestItem.Value.(cacheEntry)
	entry.Nodes = cloneNodes(entry.Nodes)
	entry.VectorClock = entry.VectorClock.Clone()
	return entry, true
}

func (c *stateCache) Put(docID types.DocumentID, entry cacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := cacheKey{Document: docID, LSN: entry.LSN}
	if element, ok := c.items[key]; ok {
		element.Value = entry
		c.ll.MoveToFront(element)
		return
	}

	element := c.ll.PushFront(entry)
	c.items[key] = element

	if c.ll.Len() > c.capacity {
		last := c.ll.Back()
		if last != nil {
			c.ll.Remove(last)
			for k, v := range c.items {
				if v == last {
					delete(c.items, k)
					break
				}
			}
		}
	}
}

func cloneNodes(nodes []crdt.CharacterNode) []crdt.CharacterNode {
	clone := make([]crdt.CharacterNode, len(nodes))
	copy(clone, nodes)
	return clone
}
