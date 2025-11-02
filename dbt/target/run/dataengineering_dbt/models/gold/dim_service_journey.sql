
  
    
    
    
        
         


        insert into `default`.`dim_service_journey__dbt_backup`
        ("service_journey_id", "date_id", "direction", "VehicleMode", "line_id", "route_id")-- Expanded ServiceJourney dimension
select distinct
  ServiceJourneyId as service_journey_id,
  cast(AimedArrivalTime as Date) as date_id,
  DirectionRef as direction,
  VehicleMode,
  LineId as line_id,
  RouteId as route_id
from `default`.`silver_timetable`
where ServiceJourneyId is not null
  