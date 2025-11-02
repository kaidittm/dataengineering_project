

  create or replace view `default`.`silver_lines` 
  
    
  
  
    
    
  as (
    -- Silver layer: deduplicate lines with route and direction
select
  id,
  name,
  public_code,
  route_id,
  direction,
  Ingestion_Date
from `default`.`stg_netex_lines`
group by id, name, public_code, route_id, direction, Ingestion_Date
    
  )
      
      
                    -- end_of_sql
                    
                    