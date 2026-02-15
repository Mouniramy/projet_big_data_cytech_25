-- Suppression des tables si elles existent (pour réexécution)
DROP TABLE IF EXISTS fact_trips;
DROP TABLE IF EXISTS dim_vendor;
DROP TABLE IF EXISTS dim_location;

-- Dimension des Vendeurs
CREATE TABLE dim_vendor (
    vendor_id INT PRIMARY KEY,
    vendor_name VARCHAR(50)
);

-- Dimension des Localisations (Normalisée en flocon avec Zone)
CREATE TABLE dim_location (
    location_id INT PRIMARY KEY,
    borough VARCHAR(50),
    zone VARCHAR(100)
);

-- Table de Faits : Les trajets de taxi
CREATE TABLE fact_trips (
    trip_id SERIAL PRIMARY KEY,
    vendor_id INT REFERENCES dim_vendor(vendor_id),
    pickup_location_id INT REFERENCES dim_location(location_id),
    dropoff_location_id INT REFERENCES dim_location(location_id),
    passenger_count INT,
    trip_distance FLOAT,
    total_amount FLOAT
);