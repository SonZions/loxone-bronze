-- Receipt time selects the mapping. Ingest time only orders catch-up work.
-- A ledger mismatch automatically revisits messages after a late structure arrives.
CREATE OR REPLACE TABLE ${silver}._refresh_batch AS
WITH versions AS (
    SELECT *, row_number() OVER (PARTITION BY source_id ORDER BY structure_captured_at, structure_id) AS rn,
           lead(structure_captured_at) OVER (PARTITION BY source_id ORDER BY structure_captured_at, structure_id) AS valid_to
    FROM ${silver}.structure_versions
), desired AS (
    SELECT b.message_id, b.source_id, b.received_at, b.message_type, b.ingested_at,
           json_extract(b.payload_json, '$.parsed') AS parsed,
           s.structure_id, s.structure_captured_at, s.structure_last_modified_raw,
           CASE WHEN s.structure_id IS NULL THEN 'no_valid_structure'
                WHEN b.received_at < s.structure_captured_at THEN 'fallback_earliest'
                ELSE 'asof' END AS structure_resolution
    FROM ${bronze}.ws_messages b
    LEFT JOIN versions s ON s.source_id=b.source_id
      AND (s.rn=1 OR b.received_at >= s.structure_captured_at)
      AND (s.valid_to IS NULL OR b.received_at < s.valid_to)
    WHERE b.message_type IN (2,3)
      AND json_type(b.payload_json, '$.parsed')='OBJECT'
      AND len(json_keys(b.payload_json, '$.parsed')) > 0
)
SELECT d.* FROM desired d
WHERE NOT EXISTS (
    SELECT 1 FROM ${silver}.processed_messages p
    WHERE p.source_id=d.source_id AND p.message_id=d.message_id
      AND p.structure_id IS NOT DISTINCT FROM d.structure_id
)
ORDER BY d.ingested_at, d.source_id, d.message_id
LIMIT ${batch_size};

CREATE OR REPLACE TABLE ${silver}._refresh_events AS
WITH meanings AS (
    SELECT DISTINCT structure_id, state_uuid, state_name, control_uuid,
           control_name, control_type, room_uuid, room_name, category_uuid, category_name,
           mapping_source, mapping_priority
    FROM ${silver}.state_uuid_map
), best AS (
    SELECT * FROM meanings
    QUALIFY mapping_priority = min(mapping_priority) OVER (PARTITION BY structure_id,state_uuid)
), selected AS (
    SELECT *, count(*) OVER (PARTITION BY structure_id,state_uuid)::INTEGER AS competing_best_mappings
    FROM best
    QUALIFY row_number() OVER (PARTITION BY structure_id,state_uuid
        ORDER BY control_uuid, state_name, mapping_source, control_name) = 1
), exploded AS (
    SELECT b.*, e.key AS state_uuid, e.value AS value_json, e.type AS value_type,
           row_number() OVER (PARTITION BY b.source_id,b.message_id,e.key ORDER BY e.id DESC) AS duplicate_rank
    FROM ${silver}._refresh_batch b, json_each(b.parsed) e
)
SELECT e.received_at, timezone('Europe/Berlin',e.received_at) AS received_at_berlin,
       e.source_id,e.message_id,e.message_type,e.state_uuid,
       m.state_name,m.control_uuid,m.control_name,m.control_type,
       m.room_uuid,m.room_name,m.category_uuid,m.category_name,
       CASE WHEN e.value_type IN ('DOUBLE','BIGINT','UBIGINT','BOOLEAN')
            THEN try_cast(e.value_json AS DOUBLE) END AS value_numeric,
       CASE WHEN e.value_type='VARCHAR' THEN json_extract_string(e.value_json,'$') END AS value_text,
       e.value_json,e.structure_id,e.structure_captured_at,
       e.structure_last_modified_raw AS structure_last_modified,e.structure_resolution,
       m.mapping_source,m.mapping_priority,coalesce(m.competing_best_mappings,0) AS competing_best_mappings,
       CASE WHEN m.structure_id IS NULL THEN 'unresolved'
            WHEN m.competing_best_mappings > 1 THEN 'ambiguous'
            WHEN m.mapping_source='state' THEN 'resolved_state'
            WHEN m.mapping_source='details' THEN 'resolved_details'
            WHEN m.mapping_source='uuidAction' THEN 'resolved_uuid_action'
            ELSE 'resolved_control' END AS resolution_status,
       e.ingested_at AS bronze_ingested_at
FROM exploded e LEFT JOIN selected m
  ON m.structure_id=e.structure_id AND m.state_uuid=e.state_uuid
WHERE e.duplicate_rank=1;

-- Replacement and acknowledgement are in the SAME transaction. No truncate.
DELETE FROM ${silver}.state_events e USING ${silver}._refresh_batch b
WHERE e.source_id=b.source_id AND e.message_id=b.message_id;
INSERT INTO ${silver}.state_events BY NAME SELECT * FROM ${silver}._refresh_events;
DELETE FROM ${silver}.processed_messages p USING ${silver}._refresh_batch b
WHERE p.source_id=b.source_id AND p.message_id=b.message_id;
INSERT INTO ${silver}.processed_messages (source_id,message_id,structure_id)
SELECT source_id,message_id,structure_id FROM ${silver}._refresh_batch;
