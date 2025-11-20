select
  row_number() over (
      order by s.ServiceJourneyId, s.QuayId, s.AimedArrivalTime
  ) as event_id,
  sp.StopPlaceId as stop_place_id,
  --sp.StopPointId as stop_point_id,
  cast(s.AimedArrivalTime as Date) as date_id,
  sp.Name as stop_point_name,
  sp.ShortName as stop_point_short_name,
  sp.Centroid_Long,
  sp.Centroid_Lat,
  sp.StopPlaceType,
  sp.ParentStopPlaceId,
  sp.Ingestion_Date
from {{ ref('silver_stop_places') }} as sp
left join {{ ref('silver_timetable') }} as s
  --on s.StopPointId = sp.StopPointId
  on s.StopPointId = sp.StopPlaceId
where sp.StopPlaceId is not null