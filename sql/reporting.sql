CREATE SCHEMA IF NOT EXISTS reporting;
CREATE OR REPLACE VIEW reporting.flight AS
SELECT
    *,
    CASE WHEN dep_delay_new > 0 THEN 1 ELSE 0 END AS is_delayed
FROM public.flight
WHERE cancelled = 0;


CREATE OR REPLACE VIEW reporting.top_reliability_roads AS
SELECT
    f.origin_airport_id AS origin_id,
    al.name as origin_airport_name,
    f.dest_airport_id AS dest_id,
    al.display_airport_name as dest_airport_name,
    f.year,
    COUNT(*) AS cnt,
    ROUND(AVG(CASE WHEN f.dep_delay_new > 0 THEN 1 ELSE 0 END) * 100, 2) AS reliability,
    ROW_NUMBER() OVER (PARTITION BY f.origin_airport_id, f.dest_airport_id, f.year ORDER BY COUNT(*) DESC) AS nb
FROM
    public.flight f
JOIN
    public.airport_list al ON f.origin_airport_id = al.origin_airport_id
WHERE
    f.cancelled = 0
GROUP BY
    f.origin_airport_id, al.name, f.dest_airport_id, al.display_airport_name, f.year
HAVING
    COUNT(*) > 10000;
CREATE OR REPLACE VIEW reporting.year_to_year_comparision AS
SELECT 
    month AS month_column_name,
    year AS year_column_name,
    COUNT(*) AS flights_amount,
    (SUM(CASE WHEN dep_delay_new > 0 THEN 1 ELSE 0 END)::decimal / COUNT(*)) * 100 AS reliability
FROM flight
GROUP BY year, month;

CREATE OR REPLACE VIEW reporting.day_to_day_comparision AS
SELECT 
    year AS year_column_name,
    day_of_week AS day_of_week_column_name,
    COUNT(*) AS flights_amount
FROM flight
GROUP BY year, day_of_week;

CREATE OR REPLACE VIEW reporting.day_by_day_reliability AS
SELECT
    TO_DATE(CONCAT(year, LPAD(month::TEXT, 2, '0'), LPAD(day_of_week::TEXT, 2, '0')), 'YYYYMMDD') AS date,
    COUNT(*) FILTER(WHERE dep_delay_new > 0) * 100.0 / COUNT(*) AS reliability
FROM flight
GROUP BY year, month, day_of_week