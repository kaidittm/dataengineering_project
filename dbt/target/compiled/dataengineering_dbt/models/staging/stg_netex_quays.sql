-- Staging model: NetEx quays from bronze
select
  id as QuayId,
  JSONExtractString(payload, 'Name') as QuayName,
  JSONExtractString(payload, 'stopPlaceId') as ParentStopPointId,
  -- Location composed from centroid latitude/longitude if present in the payload
  concat(
    coalesce(JSONExtractString(payload, 'Centroid_Lat'), ''),
    ',',
    coalesce(JSONExtractString(payload, 'Centroid_Long'), '')
  ) as Location,
  Ingestion_Date
from bronze_netex_quays
where id is not null