# sync-vector-engine


-----

### 1\. The Elevator Pitch

A high-throughput, conflict-free synchronization engine capable of managing the state of a text document across multiple unstable clients in real-time, using CRDTs (Conflict-free Replicated Data Types) to guarantee eventual consistency without a central lock.

### 2\. Core Philosophy

  * **Backend Authority:** The frontend is a "dumb view." The backend is the brain that determines the canonical state of the document.
  * **Lock-Free:** No user is ever blocked from typing. The system resolves chaos mathematically.
  * **Event-Driven:** The state of the document is not a static string; it is the sum of all historical events.

### 3\. Functional Specifications (Features)

#### Phase 1: The Engine (MVP)

  * **WebSocket Gateway:** A raw WebSocket server (no Socket.io) handling connection upgrades, heartbeats, and binary message framing.
  * **CRDT Implementation:** A custom implementation of a Sequence CRDT (like LSEQ or RGA). You must implement the algorithm that generates unique, sortable IDs for every character inserted.
  * **Broadcasting:** A Pub/Sub mechanism to instantly push "deltas" (changes) from one client to all others connected to the same `document_id`.
  * **Presence System:** An ephemeral state tracker that broadcasts metadata: "User X is at Line 10, Column 5."

#### Phase 2: The Complexity (The "Flex")

  * **Vector Clock History:** The server tracks the "logical time" of every client. This allows the server to detect if a client is sending "stale" data (e.g., an edit made 5 seconds ago while they lost internet) and merge it correctly into the present.
  * **Snapshotting:** The system monitors the operation log. When the log for a document exceeds $N$ entries, a background worker compiles the current state into a static snapshot and clears the RAM, storing the snapshot in object storage (simulated S3/MinIO).
  * **Playback API:** An endpoint that allows a client to request the document state *at a specific point in time* (e.g., "Show me the code as it looked at 10:45 AM").

### 4\. Non-Functional Requirements (Constraints)

  * **Latency Budget:** The "Time to First Byte" for a broadcast message (User A types -\> Server -\> User B receives) must be **\< 50ms** within the same region.
  * **Concurrency:** A single server instance must handle **1,000 concurrent open connections** without crashing or significant GC (Garbage Collection) pauses.
  * **Persistence:** No data loss allowed. Every operation must be written to an append-only log (WAL) before it is acknowledged to the client.
  * **Protocol:** Communication must use **Binary Protobufs** or a custom compact binary format, not JSON, to minimize bandwidth usage.

### 5\. Technical Constraints & Stack

To ensure this remains a "Hard Backend" project:

  * **Language:** **Go** (Golang) or **Rust**.
      * *Why:* You need manual control over threads/goroutines and memory layout to handle the CRDT structures efficiently.
  * **Database:** **PostgreSQL** (for the append-only event log) + **Redis** (for ephemeral Pub/Sub and Presence).
  * **The "Forbidden" List:**
      * No using `Y.js`, `Automerge`, or `ShareDB`. You must write the merging logic yourself.
      * No HTTP polling. Strictly WebSockets.

### 6\. The Data Structure (The Core Challenge)

Instead of storing a string `"Hello"`, your backend memory will store a Tree or Linked List of nodes.

**The Node Spec:**

```text
Struct CharacterNode {
    ID:          {Timestamp + ClientID} (Global Unique Identifier)
    OriginLeft:  ID of the character strictly to the left when inserted
    OriginRight: ID of the character strictly to the right when inserted
    Content:     Byte (The letter 'A')
    IsDeleted:   Boolean (Tombstone mechanism - we never actually delete memory)
}
```

*The Constraint:* When User A and User B insert at the same spot simultaneously, your backend uses the `ID` comparison to deterministically decide who goes first, ensuring all users see the same result eventually.



