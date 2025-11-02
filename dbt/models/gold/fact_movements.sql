
-- Expanded star schema fact table for Events
select
  row_number() over (order by ServiceJourneyId, QuayId, AimedArrivalTime) as event_id,
  ServiceJourneyId as service_journey_id,
  QuayId as quay_id,
  StopPointId as stop_point_id,
  cast(AimedArrivalTime as Date) as date_id,
  LineId as line_id,
  RouteId as route_id,
  DirectionRef as direction,
  VehicleMode,
  AimedArrivalTime,
  ActualArrivalTime,
  AimedDepartureTime,
  ActualDepartureTime,
  ArrivalStatus,
  DepartureStatus,
  Cancellation,
  DepartureBoardingActivity,
  Ingestion_Date
from {{ ref('silver_timetable') }}
