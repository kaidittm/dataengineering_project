
  
    
    
    
        
         


        insert into `default`.`dim_line__dbt_backup`
        ("line_id", "name", "public_code", "route_id", "direction")-- Expanded Line dimension
select
  id as line_id,
  name,
  public_code,
  route_id,
  direction
from `default`.`silver_lines`
  