-- Expanded ServiceJourney dimension
select distinct
  ServiceJourneyId as service_journey_id,
  cast(AimedArrivalTime as Date) as date_id,
  DirectionRef as direction,
  VehicleMode,
  LineId as line_id,
  RouteId as route_id
from `default`.`silver_timetable`
where ServiceJourneyId is not null