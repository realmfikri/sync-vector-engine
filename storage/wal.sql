-- Document operations write-ahead log schema.
-- Partitioned and indexed for fast append and replay workloads.

CREATE TABLE IF NOT EXISTS document_operations (
    lsn BIGSERIAL PRIMARY KEY,
    document_id TEXT NOT NULL,
    op_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    vector_clock JSONB NOT NULL,
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
) PARTITION BY HASH (document_id);

-- Hash partitions keep append performance predictable across documents.
CREATE TABLE IF NOT EXISTS document_operations_p0 PARTITION OF document_operations FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE IF NOT EXISTS document_operations_p1 PARTITION OF document_operations FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE IF NOT EXISTS document_operations_p2 PARTITION OF document_operations FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE IF NOT EXISTS document_operations_p3 PARTITION OF document_operations FOR VALUES WITH (MODULUS 4, REMAINDER 3);

-- Unique per-document operation identifiers keep replay idempotent.
CREATE UNIQUE INDEX IF NOT EXISTS document_operations_doc_op_idx
    ON document_operations USING btree (document_id, op_id);

-- Ordered scans by LSN support incremental replay and checkpointing.
CREATE INDEX IF NOT EXISTS document_operations_doc_lsn_idx
    ON document_operations USING btree (document_id, lsn DESC);

CREATE TABLE IF NOT EXISTS document_checkpoints (
    document_id TEXT PRIMARY KEY,
    last_lsn BIGINT NOT NULL,
    checkpointed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
