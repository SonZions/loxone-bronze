-- Every work table is explicitly in the cloud target database, never TEMP/local.
CREATE OR REPLACE TABLE ${silver}._refresh_structures AS
SELECT b.* FROM ${bronze}.structures b
WHERE length(cast(b.payload_json AS VARCHAR)) > 1000
  AND json_type(b.payload_json, '$.controls') = 'OBJECT'
  AND len(json_keys(b.payload_json, '$.controls')) > 0
  AND NOT EXISTS (SELECT 1 FROM ${silver}.structure_versions s
                  WHERE s.structure_id = b.structure_id)
ORDER BY b.captured_at,b.structure_id
LIMIT 1;

CREATE OR REPLACE TABLE ${silver}._refresh_controls AS
WITH RECURSIVE controls AS (
    SELECT b.structure_id, e.key AS control_key, e.value AS obj,
           '$.controls.' || e.key AS control_path,
           json_extract_string(e.value, '$.room') AS room_uuid,
           json_extract_string(e.value, '$.cat') AS category_uuid
    FROM ${silver}._refresh_structures b,
         json_each(b.payload_json, '$.controls') e
    WHERE e.type = 'OBJECT'
    UNION ALL
    SELECT c.structure_id, e.key, e.value,
           c.control_path || '.subControls.' || e.key,
           coalesce(json_extract_string(e.value, '$.room'), c.room_uuid),
           coalesce(json_extract_string(e.value, '$.cat'), c.category_uuid)
    FROM controls c, json_each(c.obj, '$.subControls') e
    WHERE e.type = 'OBJECT'
)
SELECT c.*, b.captured_at AS structure_captured_at,
       b.last_modified AS structure_last_modified,
       json_extract_string(c.obj, '$.name') AS control_name,
       json_extract_string(c.obj, '$.type') AS control_type,
       json_extract_string(b.payload_json, '/rooms/' || c.room_uuid || '/name') AS room_name,
       json_extract_string(b.payload_json, '/cats/' || c.category_uuid || '/name') AS category_name
FROM controls c JOIN ${silver}._refresh_structures b USING(structure_id);

INSERT INTO ${silver}.state_uuid_map BY NAME
WITH candidates AS (
    SELECT c.*, e.key AS state_name, json_extract_string(e.value, '$') AS state_uuid,
           'state' AS mapping_source, 1 AS mapping_priority
    FROM ${silver}._refresh_controls c, json_each(c.obj, '$.states') e
    WHERE e.type = 'VARCHAR'
    UNION ALL
    SELECT c.*, e.key, json_extract_string(e.value, '$'), 'details', 2
    FROM ${silver}._refresh_controls c, json_tree(c.obj, '$.details') e
    WHERE e.type = 'VARCHAR'
    UNION ALL
    SELECT c.*, 'uuidAction', json_extract_string(c.obj, '$.uuidAction'), 'uuidAction', 3
    FROM ${silver}._refresh_controls c
    UNION ALL
    SELECT c.*, 'control', c.control_key, 'controlKey', 4
    FROM ${silver}._refresh_controls c
)
SELECT DISTINCT c.structure_id, c.structure_captured_at,
       c.structure_last_modified, c.state_uuid AS state_uuid,
       c.state_name, c.control_key AS control_uuid,
       c.control_name, c.control_type,
       c.room_uuid, c.room_name, c.category_uuid, c.category_name,
       c.mapping_source, c.mapping_priority, c.control_path
FROM candidates c
WHERE c.state_uuid IS NOT NULL AND (c.mapping_source != 'details' OR regexp_full_match(c.state_uuid, '(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{16}'));

INSERT INTO ${silver}.structure_versions BY NAME
SELECT b.structure_id, b.source_id, b.captured_at AS structure_captured_at,
       b.last_modified AS structure_last_modified_raw,
       try_cast(b.last_modified AS TIMESTAMP) AS structure_last_modified,
       b.payload_sha256, b.collector_version, b.ingested_at,
       octet_length(encode(cast(b.payload_json AS VARCHAR))) AS payload_bytes,
       len(json_keys(b.payload_json, '$.controls')) AS control_count,
       (SELECT count(*) FROM ${silver}._refresh_controls c WHERE c.structure_id=b.structure_id) AS control_object_count,
       (SELECT count(*) FROM ${silver}.state_uuid_map m WHERE m.structure_id=b.structure_id) AS state_mapping_count,
       len(json_keys(b.payload_json, '$.rooms')) AS room_count,
       len(json_keys(b.payload_json, '$.cats')) AS category_count
FROM ${silver}._refresh_structures b;
