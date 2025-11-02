-- Expanded Quay dimension
-- Use NetEx quays staging model for quay metadata (name, parent stop point, location)
select distinct
  QuayId as quay_id,
  StopPointId as stop_point_id,
  QuayName as quay_name,
  Location as location,
  ParentStopPointId as parent_stop_point_id
from `default`.`stg_netex_quays` q
left join `default`.`silver_timetable` s
  on q.QuayId = s.QuayId
where q.QuayId is not null