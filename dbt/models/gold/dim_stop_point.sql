select
  row_number() over (order by s.ServiceJourneyId, s.QuayId, s.AimedArrivalTime) as event_id,
  sp.stop_point_id,
  cast(s.AimedArrivalTime as Date) as date_id,
  sp.name as stop_point_name,
  sp.short_name as stop_point_short_name,
  sp.centroid_long,
  sp.centroid_lat,
  sp.stop_place_type,
  sp.parent_stop_place_id,
  sp.ingestion_date
from {{ ref('silver_stop_places') }} as sp
left join {{ ref('silver_timetable') }} as s
  on s.StopPointId = sp.stop_point_id
where sp.stop_point_id is not null