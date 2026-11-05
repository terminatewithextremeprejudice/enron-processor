-- 0001_init.up.sql — canonical schema for the Enron mail processor.
-- Transactional: psql -v ON_ERROR_STOP=1 either applies all of it or none of it.

BEGIN;

-- ============================================================================
-- person — one row per distinct e-mail identity (sender or recipient).
--
-- Identity convention: the e-mail address (lower-cased/trimmed at write time
-- by the worker) is the identity; the display name is best-effort metadata
-- that may legitimately be '' (Enron mails sometimes only carry an address).
-- Two humans sharing an address in this dataset are one row — by design.
-- ============================================================================
CREATE TABLE IF NOT EXISTS person (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        TEXT        NOT NULL DEFAULT '',
    email       TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_person_email UNIQUE (email)
);

COMMENT ON TABLE person IS 'Distinct e-mail identities seen in the dataset; identity = email address, display name best-effort.';

-- ============================================================================
-- message — one row per parsed e-mail.
--
-- idempotency_key is the pipeline-wide dedupe id (sha256 of the
-- dataset-relative mail path) defined by the producer and honoured here.
-- Duplicate delivery from Kafka or DLQ replay can never create a second row.
--
-- sender is a plain ONE-TO-MANY relation onto person (one person sends many
-- messages; a message has exactly one sender).
-- ============================================================================
CREATE TABLE IF NOT EXISTS message (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    idempotency_key TEXT        NOT NULL,
    source_path     TEXT        NOT NULL DEFAULT '',
    message_id      TEXT        NOT NULL DEFAULT '',
    subject         TEXT        NOT NULL DEFAULT '',
    body_text       TEXT        NOT NULL DEFAULT '',
    body_html       TEXT        NOT NULL DEFAULT '',
    sender_id       BIGINT      NOT NULL REFERENCES person (id) ON DELETE RESTRICT,
    sent_at         TIMESTAMPTZ,
    received_at     TIMESTAMPTZ,
    parsed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_message_idempotency UNIQUE (idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_message_sender    ON message (sender_id);
CREATE INDEX IF NOT EXISTS idx_message_parsed_at ON message (parsed_at);

COMMENT ON TABLE message IS 'Parsed Enron mails; idempotency_key enforces effective-once ingestion.';
COMMENT ON COLUMN message.sender_id IS 'One-to-many: one sender person, many messages.';

-- ============================================================================
-- message_recipient — many-to-many bridge between message and person.
-- One row per (message, recipient). If an address appears in both To and Cc
-- of the same mail it is stored once (type 'to' wins, deduped by PK).
-- recipient_type preserves which header the address came from.
-- ============================================================================
CREATE TABLE IF NOT EXISTS message_recipient (
    message_id     BIGINT NOT NULL REFERENCES message (id) ON DELETE CASCADE,
    person_id      BIGINT NOT NULL REFERENCES person (id) ON DELETE CASCADE,
    recipient_type TEXT   NOT NULL DEFAULT 'to'
                   CHECK (recipient_type IN ('to', 'cc', 'bcc')),
    PRIMARY KEY (message_id, person_id)
);

CREATE INDEX IF NOT EXISTS idx_message_recipient_person ON message_recipient (person_id);

COMMENT ON TABLE message_recipient IS 'Many-to-many bridge: message <-> recipient person, with To/Cc/Bcc provenance.';

-- ============================================================================
-- envelope_receipt — raw-level idempotency/audit table.
--
-- Records every envelope key the pipeline saw across the wire (producer
-- produce, DLQ replay detection). Distinguished from message.idempotency_key:
-- this proves the RAW stage ran; message dedup proves the PERSIST stage ran.
-- ============================================================================
CREATE TABLE IF NOT EXISTS envelope_receipt (
    envelope_key   TEXT        PRIMARY KEY,
    source_path    TEXT        NOT NULL DEFAULT '',
    topic          TEXT        NOT NULL DEFAULT 'enron.raw',
    first_seen     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_component TEXT        NOT NULL DEFAULT 'producer'
);

COMMENT ON TABLE envelope_receipt IS 'Raw-stage idempotency/audit: envelope keys observed on Kafka, with first/last sighting.';

COMMIT;
