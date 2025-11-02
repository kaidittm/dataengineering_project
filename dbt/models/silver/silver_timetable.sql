
-- Silver layer: clean timetable with all required fields
select
  ServiceJourneyId,
  QuayId,
  StopPointId,
  cast(AimedArrivalTime as Date) as DateId,
  LineId,
  RouteId,
  DirectionRef,
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
from {{ ref('stg_live_timetable') }}
where ActualArrivalTime is not null
