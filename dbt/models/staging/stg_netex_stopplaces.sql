-- Staging model: NeTEx stop places from bronze
-- Expanded to expose all Bronze columns expected by downstream silver/gold models.
SELECT
  --id,
  JSONExtractString(payload, 'id') AS StopPlaceId,
--  JSONExtractString(payload, 'StopPointId') AS StopPointId,
  JSONExtractString(payload, 'Name') AS Name,
  JSONExtractString(payload, 'ShortName') AS ShortName,
  JSONExtractFloat(payload, 'Centroid_Long') AS Centroid_Long,
  JSONExtractFloat(payload, 'Centroid_Lat') AS Centroid_Lat,
  JSONExtractString(payload, 'StopPlaceType') AS StopPlaceType,
  JSONExtractString(payload, 'ParentStopPlaceId') AS ParentStopPlaceId,
  Ingestion_Date AS Ingestion_Date
FROM bronze_netex_stop_places
WHERE JSONExtractString(payload, 'id') IS NOT NULL