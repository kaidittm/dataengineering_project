-- Check if the roles were created
SELECT 'Verification of Roles';
SHOW ROLES;

-- Check if the views were created
SELECT 'Verification of Views';
SHOW TABLES LIKE 'view_fact_movements_%';

-- Test the Full View 
SELECT 'Test View: view_fact_movements_full';
SELECT
    service_journey_id,
    quay_id,
    AimedArrivalTime
FROM view_fact_movements_full
LIMIT 3;

-- Test the Limited View 
SELECT 'Test View: view_fact_movements_limited';
SELECT
    service_journey_id_hashed,
    quay_id_hashed,
    AimedArrivalTime
FROM view_fact_movements_limited
LIMIT 3;
