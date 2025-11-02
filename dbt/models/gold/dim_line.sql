
-- Expanded Line dimension
select
  id as line_id,
  name,
  public_code,
  route_id,
  direction
from {{ ref('silver_lines') }}
