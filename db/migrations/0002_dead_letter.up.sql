-- 0002_dead_letter.up.sql — dead letter table for failed parses

BEGIN;

-- dead_letter — records envelopes that failed to parse during ingestion.
--
-- idempotency_key is the same hash used by the producer (sha256 of the
-- dataset-relative mail path). This lets us detect and dedupe DLQ replay.
CREATE TABLE IF NOT EXISTS dead_letter (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    idempotency_key TEXT        NOT NULL,
    source_path     TEXT        NOT NULL DEFAULT '',
    error           TEXT        NOT NULL DEFAULT '',
    parsed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_dead_letter_idempotency UNIQUE (idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_source_path ON dead_letter (source_path);

COMMENT ON TABLE dead_letter IS 'Parse failures: envelopes that could not be decoded/persisted.';

COMMIT;
