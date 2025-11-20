-- Staging model: SIRI live timetable from bronze
-- Expanded to expose all Bronze columns expected by downstream silver/gold models.
select
  ServiceJourneyId,
  QuayId,
  StopPointId,
  DateId,
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
from bronze_live_timetable
where ServiceJourneyId is not null