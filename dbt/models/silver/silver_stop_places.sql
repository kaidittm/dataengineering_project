-- Silver layer: clean stop places with all required fields
select
  StopPlaceId,
  --StopPointId,
  Name,
  ShortName,
  Centroid_Long,
  Centroid_Lat,
  StopPlaceType,
  ParentStopPlaceId,
  Ingestion_Date
from {{ ref('stg_netex_stopplaces') }}