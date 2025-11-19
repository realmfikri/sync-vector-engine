package crdt

// CharacterNode holds the metadata and content for a single CRDT element.
// The node retains tombstones instead of being removed to preserve ordering
// information required for replication and playback.
type CharacterNode struct {
	ID          Identifier
	OriginLeft  Identifier
	OriginRight Identifier
	Content     string
	IsDeleted   bool
}

// CompareNodes compares two nodes by their identifiers.
// It returns -1 when a < b, 1 when a > b, and 0 when they are identical.
func CompareNodes(a, b CharacterNode) int {
	return a.ID.Compare(b.ID)
}

// NodeLess reports whether a's identifier is ordered before b.
func NodeLess(a, b CharacterNode) bool {
	return CompareNodes(a, b) < 0
}

// IdentifierEqual reports whether two nodes share the same identifier.
func IdentifierEqual(a, b CharacterNode) bool {
	return a.ID.Equals(b.ID)
}
