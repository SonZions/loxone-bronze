-- Run once against my_db.
-- Existing loxone and solar schemas are not touched.

CREATE SCHEMA IF NOT EXISTS loxone_bronze;

CREATE TABLE IF NOT EXISTS loxone_bronze.ws_messages (
    message_id VARCHAR PRIMARY KEY,
    source_id VARCHAR NOT NULL,
    run_id VARCHAR NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    message_type INTEGER NOT NULL,
    payload_format VARCHAR NOT NULL,
    payload_json JSON NOT NULL,
    payload_sha256 VARCHAR NOT NULL,
    collector_version VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS loxone_bronze.structures (
    structure_id VARCHAR PRIMARY KEY,
    source_id VARCHAR NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    last_modified VARCHAR,
    payload_json JSON NOT NULL,
    payload_sha256 VARCHAR NOT NULL,
    collector_version VARCHAR NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

COMMENT ON TABLE loxone_bronze.ws_messages IS
'Bronze mirror of Loxone WebSocket payloads. Raw transport payload is preserved in payload_json; parsed representation is included where supported.';

COMMENT ON TABLE loxone_bronze.structures IS
'Versioned raw LoxAPP3.json structure snapshots, deduplicated by SHA-256.';
