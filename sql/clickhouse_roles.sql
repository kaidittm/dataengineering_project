-- DEFINE ROLES 

-- Create roles for granting specific access levels
CREATE ROLE IF NOT EXISTS analyst_full;
CREATE ROLE IF NOT EXISTS analyst_limited;

-- CREATE ANALYTICAL VIEWS

-- Full Access View: Shows all data columns in their original form (for the 'analyst_full' role)
CREATE OR REPLACE VIEW view_fact_movements_full AS
SELECT
    *
FROM fact_movements;

-- Limited Access View: Pseudonymizes 3 chosen columns (for the 'analyst_limited' role)
CREATE OR REPLACE VIEW view_fact_movements_limited AS
SELECT
    -- pseudonymized columns: Event ID, Service Journey ID (unique trip identifier), Quay ID (specific stop location)
    toString(sipHash64(event_id)) AS event_id_hashed,
    toString(sipHash64(service_journey_id)) AS service_journey_id_hashed,
    toString(sipHash64(quay_id)) AS quay_id_hashed,

    -- all other columns as were
    date_id,
    AimedArrivalTime,
    ActualArrivalTime,
    AimedDepartureTime,
    ActualDepartureTime,
    Ingestion_Date, 

FROM fact_movements;

-- GRANT ACCESS TO ROLES (Implementing Least Privilege)

-- Grant the 'analyst_full' role complete SELECT access to the full view
GRANT SELECT ON view_fact_movements_full TO analyst_full;
-- Grant the 'analyst_limited' role access only to the pseudonymized view
GRANT SELECT ON view_fact_movements_limited TO analyst_limited;

-- Ensure the limited role cannot bypass the masking by accessing other views or the base table
REVOKE SELECT ON view_fact_movements_full FROM analyst_limited;
REVOKE SELECT ON fact_movements FROM analyst_limited;
