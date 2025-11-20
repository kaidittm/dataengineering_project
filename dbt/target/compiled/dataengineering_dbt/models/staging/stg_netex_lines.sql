-- Staging model: NetEx lines from bronze
select
  id,
  -- Use ClickHouse JSON extraction functions instead of parse_json(...) syntax
  -- JSONExtractString returns the string value for the given key
  JSONExtractString(payload, 'Name') as name,
  JSONExtractString(payload, 'PublicCode') as public_code,
  -- Optional route / direction fields (may be absent in some NetEx payloads)
  JSONExtractString(payload, 'RouteRef') as route_id,
  JSONExtractString(payload, 'DirectionRef') as direction,
  Ingestion_Date
from bronze_netex_lines
where id is not null