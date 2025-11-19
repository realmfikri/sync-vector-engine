package types

import (
	"encoding/json"
	"fmt"
	"time"
)

// DocumentID identifies a collaborative document.
type DocumentID string

// ClientID represents a connected client.
type ClientID string

// OperationID is a globally unique identifier for an operation.
type OperationID string

// VectorClock keeps logical time for each client participating in a document.
type VectorClock map[ClientID]uint64

// Bump increments the vector clock for a client.
func (vc VectorClock) Bump(client ClientID) {
	vc[client] = vc[client] + 1
}

// Merge merges another vector clock into the receiver by taking the max value
// for each entry.
func (vc VectorClock) Merge(other VectorClock) {
	for client, value := range other {
		if current, ok := vc[client]; !ok || value > current {
			vc[client] = value
		}
	}
}

// Compare returns true if the receiver dominates the other clock (all entries
// are greater than or equal and at least one is strictly greater).
func (vc VectorClock) Compare(other VectorClock) bool {
	var greater bool
	for client, value := range other {
		if vc[client] < value {
			return false
		}
		if vc[client] > value {
			greater = true
		}
	}
	if len(vc) > len(other) {
		greater = true
	}
	return greater
}

// WALRecord stores a durable representation of an operation.
type WALRecord struct {
	LSN         int64       `json:"lsn,omitempty"`
	Operation   OperationID `json:"operation_id"`
	Document    DocumentID  `json:"document_id"`
	Client      ClientID    `json:"client_id"`
	Payload     []byte      `json:"payload"`
	VectorClock VectorClock `json:"vector_clock"`
	CreatedAt   time.Time   `json:"created_at"`
}

// MarshalBinary serializes a WALRecord to JSON for storage in a byte-oriented
// WAL.
func (r WALRecord) MarshalBinary() ([]byte, error) {
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now().UTC()
	}
	payload := struct {
		LSN         int64       `json:"lsn,omitempty"`
		Operation   OperationID `json:"operation_id"`
		Document    DocumentID  `json:"document_id"`
		Client      ClientID    `json:"client_id"`
		Payload     string      `json:"payload"`
		VectorClock VectorClock `json:"vector_clock"`
		CreatedAt   time.Time   `json:"created_at"`
	}{
		LSN:         r.LSN,
		Operation:   r.Operation,
		Document:    r.Document,
		Client:      r.Client,
		Payload:     string(r.Payload),
		VectorClock: r.VectorClock,
		CreatedAt:   r.CreatedAt,
	}
	return json.Marshal(payload)
}

// UnmarshalBinary deserializes a WALRecord from the JSON representation.
func (r *WALRecord) UnmarshalBinary(data []byte) error {
	var payload struct {
		LSN         int64       `json:"lsn,omitempty"`
		Operation   OperationID `json:"operation_id"`
		Document    DocumentID  `json:"document_id"`
		Client      ClientID    `json:"client_id"`
		Payload     string      `json:"payload"`
		VectorClock VectorClock `json:"vector_clock"`
		CreatedAt   time.Time   `json:"created_at"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("decode wal record: %w", err)
	}
	r.LSN = payload.LSN
	r.Operation = payload.Operation
	r.Document = payload.Document
	r.Client = payload.Client
	r.Payload = []byte(payload.Payload)
	r.VectorClock = payload.VectorClock
	r.CreatedAt = payload.CreatedAt
	return nil
}
