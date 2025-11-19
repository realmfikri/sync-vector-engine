package crdt

import (
	"math/rand"
	"time"
)

// Identifier represents the position of a node within the CRDT ordering.
// Path values are compared lexicographically and tie-broken by SiteID.
type Identifier struct {
	Path   []int
	SiteID string
}

// IdentifierGenerator creates sortable identifiers between two origins.
type IdentifierGenerator struct {
	base     int
	boundary int
	siteID   string
	rng      *rand.Rand
}

// NewIdentifierGenerator initializes a generator with the provided site ID.
// The base controls the maximum branching factor of the identifier tree.
func NewIdentifierGenerator(siteID string) *IdentifierGenerator {
	return &IdentifierGenerator{
		base:     1 << 15,
		boundary: 1 << 15,
		siteID:   siteID,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Compare returns -1 if id comes before other, 1 if after, and 0 if equal.
func (id Identifier) Compare(other Identifier) int {
	minLen := len(id.Path)
	if len(other.Path) < minLen {
		minLen = len(other.Path)
	}

	for i := 0; i < minLen; i++ {
		if id.Path[i] < other.Path[i] {
			return -1
		}
		if id.Path[i] > other.Path[i] {
			return 1
		}
	}

	if len(id.Path) < len(other.Path) {
		return -1
	}
	if len(id.Path) > len(other.Path) {
		return 1
	}

	switch {
	case id.SiteID < other.SiteID:
		return -1
	case id.SiteID > other.SiteID:
		return 1
	default:
		return 0
	}
}

// Equals reports whether two identifiers refer to the same position.
func (id Identifier) Equals(other Identifier) bool {
	if len(id.Path) != len(other.Path) || id.SiteID != other.SiteID {
		return false
	}
	for i := range id.Path {
		if id.Path[i] != other.Path[i] {
			return false
		}
	}
	return true
}

// Generate produces a new identifier ordered between left and right.
func (g *IdentifierGenerator) Generate(left, right Identifier) Identifier {
	path := g.allocate(left.Path, right.Path, 0)
	return Identifier{Path: path, SiteID: g.siteID}
}

func (g *IdentifierGenerator) allocate(left, right []int, depth int) []int {
	leftDigit := g.leftDigit(left, depth)
	rightDigit := g.rightDigit(right, depth)

	if leftDigit+1 < rightDigit {
		prefix := append([]int{}, left[:depth]...)
		newDigit := leftDigit + 1 + g.rng.Intn(rightDigit-leftDigit-1)
		return append(prefix, newDigit)
	}

	prefix := append([]int{}, left[:depth]...)
	prefix = append(prefix, leftDigit)
	return g.allocate(left, right, depth+1)
}

func (g *IdentifierGenerator) leftDigit(path []int, depth int) int {
	if depth < len(path) {
		return path[depth]
	}
	return 0
}

func (g *IdentifierGenerator) rightDigit(path []int, depth int) int {
	if depth < len(path) {
		return path[depth]
	}
	return g.boundary
}
