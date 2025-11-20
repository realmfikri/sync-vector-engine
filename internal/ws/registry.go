package ws

import (
	"sync"

	proto "google.golang.org/protobuf/proto"
)

// ConnectionRegistry tracks active WebSocket connections keyed by document ID
// so downstream services can broadcast efficiently.
type ConnectionRegistry struct {
	mu        sync.RWMutex
	documents map[string]map[*Connection]struct{}
}

// NewConnectionRegistry creates an empty registry.
func NewConnectionRegistry() *ConnectionRegistry {
	return &ConnectionRegistry{documents: make(map[string]map[*Connection]struct{})}
}

// Register associates the connection with a document.
func (r *ConnectionRegistry) Register(documentID string, c *Connection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.documents[documentID] == nil {
		r.documents[documentID] = make(map[*Connection]struct{})
	}
	r.documents[documentID][c] = struct{}{}
	gatewayConnections.WithLabelValues(documentID).Set(float64(len(r.documents[documentID])))
}

// Unregister removes the connection.
func (r *ConnectionRegistry) Unregister(documentID string, c *Connection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	conns := r.documents[documentID]
	if conns == nil {
		return
	}
	delete(conns, c)
	if len(conns) == 0 {
		delete(r.documents, documentID)
	}
	gatewayConnections.WithLabelValues(documentID).Set(float64(len(conns)))
}

// BroadcastBinary delivers the payload to every connection currently attached
// to the provided document ID. The sender connection can be skipped to avoid
// echoing.
func (r *ConnectionRegistry) BroadcastBinary(documentID string, payload []byte, skip *Connection) int {
	r.mu.RLock()
	conns := r.documents[documentID]
	if len(conns) == 0 {
		r.mu.RUnlock()
		return 0
	}
	recipients := make([]*Connection, 0, len(conns))
	for c := range conns {
		if c != skip {
			recipients = append(recipients, c)
		}
	}
	r.mu.RUnlock()

	sent := 0
	for _, conn := range recipients {
		if err := conn.SendBinary(payload); err == nil {
			sent++
		}
	}
	return sent
}

// BroadcastEnvelope is a helper that marshals a protobuf envelope and forwards
// it to BroadcastBinary.
func (r *ConnectionRegistry) BroadcastEnvelope(documentID string, env proto.Message, skip *Connection) int {
	data, err := proto.Marshal(env)
	if err != nil {
		return 0
	}
	return r.BroadcastBinary(documentID, data, skip)
}

// BroadcastBinaryByClientID delivers the payload to every connection for the
// document, skipping a matching client identifier when provided. This is
// useful when relaying events received over pub/sub where the originating
// connection is not known locally.
func (r *ConnectionRegistry) BroadcastBinaryByClientID(documentID string, payload []byte, skipClientID string) int {
	r.mu.RLock()
	conns := r.documents[documentID]
	if len(conns) == 0 {
		r.mu.RUnlock()
		return 0
	}
	recipients := make([]*Connection, 0, len(conns))
	for c := range conns {
		if skipClientID != "" && c.ClientID() == skipClientID {
			continue
		}
		recipients = append(recipients, c)
	}
	r.mu.RUnlock()

	sent := 0
	for _, conn := range recipients {
		if err := conn.SendBinary(payload); err == nil {
			sent++
		}
	}
	return sent
}
