CREATE DATABASE parking_system;
USE parking_system;

# Staging layer design
CREATE TABLE stg_lots (
  lot_id_txt VARCHAR(50),
  lot_name_txt VARCHAR(100),
  area_txt VARCHAR(100),
  city_txt VARCHAR(100),
  lot_type_txt VARCHAR(50),
  rate_per_minute_txt VARCHAR(50),
  is_active_txt VARCHAR(20)
);
CREATE TABLE stg_slots (
  slot_id INT,
  lot_id INT,
  slot_number VARCHAR(20),
  slot_type VARCHAR(30),
  is_active_txt VARCHAR(20)
);

CREATE TABLE stg_events (
  event_id BIGINT,
  ticket_id VARCHAR(50),
  slot_id INT,
  vehicle_id VARCHAR(50),
  entry_time_txt VARCHAR(30),
  exit_time_txt VARCHAR(30),
  ticket_raised_time_txt VARCHAR(30),
  final_ticketraisedtime_txt VARCHAR(30),
  planned_minutes INT,
  parking_type VARCHAR(30),
  parking_minutes_clean INT,
  exit_completed_flag VARCHAR(10),
  slot_assigned_flag VARCHAR(10),
  vehicle_captured_flag VARCHAR(10),
  valid_parking_flag VARCHAR(10),
  ticket_raised_flag VARCHAR(10),
  actual_minutes_txt VARCHAR(30)
);

CREATE TABLE stg_payments (
  payment_id BIGINT,
  event_id BIGINT,
  payment_time_txt VARCHAR(30),
  payment_method VARCHAR(30),
  payment_status VARCHAR(30),
  payment_methos_clean VARCHAR(30),
  payment_status_clean VARCHAR(30),
  payment_date_TXT VARCHAR(30),
  payment_time_available_flag VARCHAR(20)
);

CREATE TABLE stg_violations (
  event_id BIGINT,
  actual_minutes_txt VARCHAR(30),
  planned_minutes_txt VARCHAR(30),
  vehicle_captured_flag VARCHAR(20),
  exit_completed_flag VARCHAR(20),
  valid_parking_flag VARCHAR(20),
  overstayed_minutes_txt VARCHAR(30),
  parking_outcome VARCHAR(40),
  penalty_factor_txt VARCHAR(30),
  slot_id INT,
  lot_id INT,
  rate_per_minute_txt VARCHAR(30),
  base_parking_amount_txt VARCHAR(30),
  penalty_amount_txt VARCHAR(30),
  capped_penalty_amount_txt VARCHAR(30),
  total_parking_charge_txt VARCHAR(30),
  final_total_parking_amount DECIMAL(12,2),
  is_towed_flag VARCHAR(20)
);



-- CREATING FINAL CLEAR TABLES

CREATE TABLE dim_lots AS
SELECT
  CAST(lot_id_txt AS UNSIGNED) AS lot_id,
  lot_name_txt AS lot_name,
  area_txt AS area,
  city_txt AS city,
  lot_type_txt AS lot_type,
  NULLIF(rate_per_minute_txt,'') + 0 AS rate_per_minute,
  CASE
    WHEN LOWER(is_active_txt) IN ('yes','y','1','true') THEN 1
    ELSE 0
  END AS is_active
FROM stg_lots;
ALTER TABLE dim_lots
  ADD PRIMARY KEY (lot_id);


CREATE TABLE dim_slots AS
SELECT
  slot_id,
  lot_id,
  slot_number,
  slot_type,
  CASE
    WHEN LOWER(is_active_txt) IN ('yes','y','1','true') THEN 1
    ELSE 0
  END AS is_active
FROM stg_slots;
ALTER TABLE dim_slots
  ADD PRIMARY KEY (slot_id);
  
CREATE TABLE fact_events AS
SELECT
  event_id,
  ticket_id,
  slot_id,
  vehicle_id,

  STR_TO_DATE(NULLIF(TRIM(entry_time_txt), ''), '%d-%m-%Y %H:%i') AS entry_time,
  STR_TO_DATE(NULLIF(TRIM(exit_time_txt), ''),  '%d-%m-%Y %H:%i') AS exit_time,
  STR_TO_DATE(NULLIF(TRIM(ticket_raised_time_txt), ''), '%d-%m-%Y %H:%i') AS ticket_raised_time,
  STR_TO_DATE(NULLIF(TRIM(final_ticketraisedtime_txt), ''), '%d-%m-%Y %H:%i') AS final_ticketraisedtime,

  planned_minutes,
  parking_type,
  parking_minutes_clean,

  CASE WHEN LOWER(TRIM(exit_completed_flag)) IN ('yes','y','1','true') THEN 1 ELSE 0 END AS exit_completed_flag,
  CASE WHEN LOWER(TRIM(slot_assigned_flag))  IN ('yes','y','1','true') THEN 1 ELSE 0 END AS slot_assigned_flag,
  CASE WHEN LOWER(TRIM(vehicle_captured_flag)) IN ('yes','y','1','true') THEN 1 ELSE 0 END AS vehicle_captured_flag,
  CASE WHEN LOWER(TRIM(valid_parking_flag)) IN ('yes','y','1','true') THEN 1 ELSE 0 END AS valid_parking_flag,
  CASE WHEN LOWER(TRIM(ticket_raised_flag)) IN ('yes','y','1','true') THEN 1 ELSE 0 END AS ticket_raised_flag,

  CAST(NULLIF(TRIM(actual_minutes_txt), '') AS UNSIGNED) AS actual_minutes
FROM stg_events;

SELECT COUNT(*) AS rows_in_fact_events FROM fact_events;

SELECT
  SUM(entry_time IS NULL) AS null_entry_time,
  SUM(exit_time IS NULL)  AS null_exit_time
FROM fact_events;



DROP TABLE IF EXISTS fact_payments;

CREATE TABLE fact_payments AS
SELECT
  payment_id,
  event_id,

  CASE
    WHEN NULLIF(TRIM(payment_time_txt), '') IS NULL THEN NULL

    -- 01-07-2022 20:26
    WHEN TRIM(payment_time_txt) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4} [0-9]{2}:[0-9]{2}' 
      THEN STR_TO_DATE(TRIM(payment_time_txt), '%d-%m-%Y %H:%i')

    -- 01-Jul-22 20:26 (if it exists)
    WHEN TRIM(payment_time_txt) REGEXP '^[0-9]{2}-[A-Za-z]{3}-[0-9]{2} [0-9]{2}:[0-9]{2}'
      THEN STR_TO_DATE(TRIM(payment_time_txt), '%d-%b-%y %H:%i')

    -- 01-Jul-22
    WHEN TRIM(payment_time_txt) REGEXP '^[0-9]{2}-[A-Za-z]{3}-[0-9]{2}$'
      THEN STR_TO_DATE(TRIM(payment_time_txt), '%d-%b-%y')

    -- 01-07-2022
    WHEN TRIM(payment_time_txt) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
      THEN STR_TO_DATE(TRIM(payment_time_txt), '%d-%m-%Y')

    ELSE NULL
  END AS payment_time,

  payment_method,
  payment_status,
  payment_methos_clean,
  payment_status_clean,

  CASE
    WHEN NULLIF(TRIM(payment_date_txt), '') IS NULL THEN NULL
    WHEN TRIM(payment_date_txt) REGEXP '^[0-9]{2}-[A-Za-z]{3}-[0-9]{2}$'
      THEN STR_TO_DATE(TRIM(payment_date_txt), '%d-%b-%y')
    WHEN TRIM(payment_date_txt) REGEXP '^[0-9]{2}-[0-9]{2}-[0-9]{4}$'
      THEN STR_TO_DATE(TRIM(payment_date_txt), '%d-%m-%Y')
    ELSE NULL
  END AS payment_date,

  CASE
    WHEN LOWER(TRIM(payment_time_available_flag)) IN ('yes','y','1','true') THEN 1
    ELSE 0
  END AS payment_time_available_flag

FROM stg_payments;


DROP TABLE IF EXISTS fact_violations;

CREATE TABLE fact_violations AS
SELECT
  event_id,

  CAST(NULLIF(TRIM(actual_minutes_txt), '') AS DECIMAL(10,2)) AS actual_minutes,
  CAST(NULLIF(TRIM(planned_minutes_txt), '') AS DECIMAL(10,2)) AS planned_minutes,

  CASE WHEN LOWER(TRIM(vehicle_captured_flag)) IN ('yes','y','1','true') THEN 1 ELSE 0 END AS vehicle_captured_flag,
  CASE WHEN LOWER(TRIM(exit_completed_flag))   IN ('yes','y','1','true') THEN 1 ELSE 0 END AS exit_completed_flag,
  CASE WHEN LOWER(TRIM(valid_parking_flag))    IN ('yes','y','1','true') THEN 1 ELSE 0 END AS valid_parking_flag,

  CAST(NULLIF(TRIM(overstayed_minutes_txt), '') AS DECIMAL(10,2)) AS overstayed_minutes,
  parking_outcome,
  CAST(NULLIF(TRIM(penalty_factor_txt), '') AS DECIMAL(10,2)) AS penalty_factor,

  slot_id,
  lot_id,

  CAST(NULLIF(TRIM(rate_per_minute_txt), '') AS DECIMAL(10,4)) AS rate_per_minute,
  CAST(NULLIF(TRIM(base_parking_amount_txt), '') AS DECIMAL(12,2)) AS base_parking_amount,
  CAST(NULLIF(TRIM(penalty_amount_txt), '') AS DECIMAL(12,2)) AS penalty_amount,
  CAST(NULLIF(TRIM(capped_penalty_amount_txt), '') AS DECIMAL(12,2)) AS capped_penalty_amount,
  CAST(NULLIF(TRIM(total_parking_charge_txt), '') AS DECIMAL(12,2)) AS total_parking_charge,

  -- NOTE: your stg_violations has final_total_parking_amount as DECIMAL already in your CREATE,
  -- but in the SELECT you're treating it like text, so we handle it safely:
  CAST(NULLIF(TRIM(CAST(final_total_parking_amount AS CHAR)), '') AS DECIMAL(12,2)) AS final_total_parking_amount,

  CASE WHEN LOWER(TRIM(is_towed_flag)) IN ('yes','y','1','true') THEN 1 ELSE 0 END AS is_towed_flag
FROM stg_violations;


SELECT 'lots' AS tbl, COUNT(*) FROM dim_lots
UNION ALL
SELECT 'slots', COUNT(*) FROM dim_slots
UNION ALL
SELECT 'events', COUNT(*) FROM fact_events
UNION ALL
SELECT 'payments', COUNT(*) FROM fact_payments
UNION ALL
SELECT 'violations', COUNT(*) FROM fact_violations;


ALTER TABLE dim_slots
MODIFY lot_id INT UNSIGNED;
ALTER TABLE dim_slots
MODIFY lot_id BIGINT UNSIGNED NOT NULL;


ALTER TABLE dim_slots
ADD CONSTRAINT fk_slots_lots
FOREIGN KEY (lot_id) REFERENCES dim_lots(lot_id);


ALTER TABLE fact_events
ADD CONSTRAINT fk_events_slots
FOREIGN KEY (slot_id) REFERENCES dim_slots(slot_id);

SELECT
  SUM(event_id IS NULL) AS null_event_id,
  COUNT(*) AS total_rows,
  COUNT(DISTINCT event_id) AS distinct_event_id
FROM fact_events;

ALTER TABLE fact_events
ADD PRIMARY KEY (event_id);

SHOW INDEX FROM fact_events;
SHOW CREATE TABLE fact_events;
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT event_id) AS distinct_event_ids,
  SUM(event_id IS NULL) AS null_event_ids
FROM fact_events;

SHOW CREATE TABLE fact_events;
SHOW INDEX FROM fact_events;

ALTER TABLE fact_payments
MODIFY event_id BIGINT NOT NULL;

DELETE p
FROM fact_payments p
LEFT JOIN fact_events e ON e.event_id = p.event_id
WHERE e.event_id IS NULL;

ALTER TABLE fact_payments
ADD CONSTRAINT fk_payments_events
FOREIGN KEY (event_id) REFERENCES fact_events(event_id);

DELETE v
FROM fact_violations v
LEFT JOIN fact_events e ON e.event_id = v.event_id
WHERE e.event_id IS NULL;

ALTER TABLE fact_violations
ADD CONSTRAINT fk_viol_events
FOREIGN KEY (event_id) REFERENCES fact_events(event_id);


SELECT 'dim_lots' AS tbl, COUNT(*) AS rows_cnt FROM dim_lots
UNION ALL SELECT 'dim_slots', COUNT(*) FROM dim_slots
UNION ALL SELECT 'fact_events', COUNT(*) FROM fact_events
UNION ALL SELECT 'fact_payments', COUNT(*) FROM fact_payments
UNION ALL SELECT 'fact_violations', COUNT(*) FROM fact_violations;


CREATE OR REPLACE VIEW vw_event_master AS
SELECT
  e.event_id,
  e.ticket_id,
  e.vehicle_id,
  e.entry_time,
  e.exit_time,
  e.actual_minutes,
  e.planned_minutes,
  e.parking_type,
  e.parking_minutes_clean,
  e.exit_completed_flag,
  e.slot_assigned_flag,
  e.vehicle_captured_flag,
  e.valid_parking_flag,
  e.ticket_raised_flag,

  s.slot_id,
  s.slot_type,
  s.lot_id,

  l.lot_name,
  l.area,
  l.city,
  l.lot_type,
  l.rate_per_minute
FROM fact_events e
JOIN dim_slots s ON s.slot_id = e.slot_id
JOIN dim_lots  l ON l.lot_id = s.lot_id;

-- KPI QUERIES

# Total sessions
SELECT COUNT(*) AS total_sessions
FROM fact_events;

# Completed sessions
SELECT COUNT(*) AS completed_sessions
FROM fact_events
WHERE exit_completed_flag = 1;

# Total parking minutes
SELECT SUM(parking_minutes_clean) AS total_parking_minutes
FROM fact_events
WHERE exit_time IS NOT NULL;

# Total revenue
SELECT SUM(final_total_parking_amount) AS total_revenue
FROM fact_violations;

# Violation Rate
SELECT
  COUNT(*) / NULLIF((SELECT COUNT(*) FROM fact_events WHERE exit_time IS NOT NULL),0) AS violation_rate
FROM fact_violations;

# Monthly Revenue Trend
SELECT
  DATE_FORMAT(e.entry_time, '%Y-%m-01') AS month_start,
  SUM(v.final_total_parking_amount) AS revenue
FROM fact_events e
JOIN fact_violations v ON v.event_id = e.event_id
WHERE e.entry_time IS NOT NULL
GROUP BY month_start
ORDER BY month_start;

# Payment Method Share
SELECT
  COALESCE(payment_methos_clean, payment_method) AS payment_method,
  COUNT(*) AS txn_count,
  100 * COUNT(*) / SUM(COUNT(*)) OVER () AS pct
FROM fact_payments
GROUP BY COALESCE(payment_methos_clean, payment_method)
ORDER BY txn_count DESC;

# Peak Hour Demand
SELECT
  HOUR(final_ticketraisedtime) AS final_ticket_raised_hour,
  COUNT(*) AS session_count
FROM fact_events
WHERE final_ticketraisedtime IS NOT NULL
GROUP BY HOUR(final_ticketraisedtime)
ORDER BY session_count DESC
LIMIT 1;
-- Peak hour differs because hours 09 and 18 have nearly identical session counts; SQL returns the true maximum (09:00 with 2074 sessions), 
-- while Power BI’s TOPN without a secondary sort may return a different hour (18:00) due to tie/ordering behavior or hour being treated as text.
SELECT
  HOUR(final_ticketraisedtime) AS hr,
  COUNT(*) AS cnt_rows,
  COUNT(event_id) AS cnt_eventid,
  COUNT(DISTINCT event_id) AS cnt_distinct_eventid
FROM fact_events
WHERE final_ticketraisedtime IS NOT NULL
GROUP BY hr
ORDER BY cnt_rows DESC
LIMIT 5;

# Average parking duration (minutes)
SELECT ROUND(AVG(actual_minutes), 2) AS avg_parking_minutes
FROM fact_events
WHERE actual_minutes IS NOT NULL;

# Daily Demand
SELECT
  DATE(entry_time) AS parking_date,
  COUNT(*) AS sessions
FROM fact_events
WHERE entry_time IS NOT NULL
GROUP BY parking_date
ORDER BY parking_date;

#  Weekday Demand
SELECT
  DAYNAME(entry_time) AS weekday,
  WEEKDAY(entry_time) AS weekday_no,   -- Monday=0 ... Sunday=6
  COUNT(*) AS sessions
FROM fact_events
WHERE entry_time IS NOT NULL
GROUP BY weekday, weekday_no
ORDER BY weekday_no;

# Hourly demand (0–23)
SELECT
  HOUR(entry_time) AS hour_of_day,
  COUNT(*) AS sessions
FROM fact_events
WHERE entry_time IS NOT NULL
GROUP BY hour_of_day
ORDER BY hour_of_day;

# Rolling 7-day demand (trend smoothing)
WITH daily AS (
  SELECT DATE(entry_time) AS d, COUNT(*) AS sessions
  FROM fact_events
  WHERE entry_time IS NOT NULL
  GROUP BY d
)
SELECT
  d,
  sessions,
  ROUND(AVG(sessions) OVER (ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS rolling_7d_avg
FROM daily
ORDER BY d;

# Week-over-week demand growth (%)
WITH weekly AS (
  SELECT
    YEAR(entry_time) AS yr,
    WEEK(entry_time, 3) AS wk,   -- ISO-like week
    COUNT(*) AS sessions
  FROM fact_events
  WHERE entry_time IS NOT NULL
  GROUP BY yr, wk
)
SELECT
  yr, wk, sessions,
  LAG(sessions) OVER (ORDER BY yr, wk) AS prev_week_sessions,
  ROUND(
    100 * (sessions - LAG(sessions) OVER (ORDER BY yr, wk))
      / NULLIF(LAG(sessions) OVER (ORDER BY yr, wk), 0),
    2
  ) AS wow_growth_pct
FROM weekly
ORDER BY yr, wk;

# Top 5 busiest lots (by sessions) with share %
WITH lot_sessions AS (
  SELECT
    l.lot_name,
    COUNT(*) AS sessions
  FROM fact_events e
  JOIN dim_slots s ON s.slot_id = e.slot_id
  JOIN dim_lots  l ON l.lot_id = s.lot_id
  GROUP BY l.lot_name
),
tot AS (SELECT SUM(sessions) AS total_sessions FROM lot_sessions)
SELECT
  ls.lot_name,
  ls.sessions,
  ROUND(100 * ls.sessions / NULLIF(t.total_sessions,0), 2) AS share_pct
FROM lot_sessions ls
CROSS JOIN tot t
ORDER BY ls.sessions DESC
LIMIT 5;

# Total Penalty Revenue
SELECT ROUND(SUM(penalty_amount), 2) AS total_penalty_revenue
FROM fact_violations;

# Revenue by Lot
SELECT
  l.lot_name,
  l.area,
  ROUND(SUM(v.final_total_parking_amount), 2) AS revenue
FROM fact_violations v
JOIN dim_lots l ON l.lot_id = v.lot_id
GROUP BY l.lot_name, l.area
ORDER BY revenue DESC;

# Payment Method Share
SELECT
  COALESCE(payment_methos_clean, payment_method) AS payment_method,
  COUNT(*) AS txn_count,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM fact_payments
GROUP BY COALESCE(payment_methos_clean, payment_method)
ORDER BY txn_count DESC;

# Top Violation Outcomes
SELECT
  parking_outcome,
  COUNT(*) AS cases,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM fact_violations
GROUP BY parking_outcome
ORDER BY cases DESC;

# Revenue uplift vs base
SELECT
  ROUND(SUM(base_parking_amount), 2) AS base_amount,
  ROUND(SUM(penalty_amount), 2) AS penalty_amount,
  ROUND(SUM(final_total_parking_amount), 2) AS total_amount,
  ROUND(100 * SUM(penalty_amount) / NULLIF(SUM(base_parking_amount), 0), 2) AS penalty_uplift_pct
FROM fact_violations;

# Pareto analysis: Top lots contributing to 80% revenue
WITH lot_rev AS (
  SELECT
    l.lot_id,
    l.lot_name,
    SUM(v.final_total_parking_amount) AS revenue
  FROM fact_violations v
  JOIN dim_lots l ON l.lot_id = v.lot_id
  GROUP BY l.lot_id, l.lot_name
),
ranked AS (
  SELECT
    lot_id, lot_name, revenue,
    SUM(revenue) OVER () AS total_revenue,
    SUM(revenue) OVER (ORDER BY revenue DESC) AS running_revenue
  FROM lot_rev
)
SELECT
  lot_name,
  ROUND(revenue, 2) AS revenue,
  ROUND(100 * running_revenue / NULLIF(total_revenue,0), 2) AS cumulative_pct
FROM ranked
ORDER BY revenue DESC;

# “Risk lots” (high overstay rate + high penalty)
SELECT
  l.lot_name,
  l.area,
  COUNT(*) AS violation_cases,
  ROUND(AVG(v.overstayed_minutes), 2) AS avg_overstay_minutes,
  ROUND(SUM(v.penalty_amount), 2) AS total_penalty,
  ROUND(100 * SUM(CASE WHEN v.is_towed_flag = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS towed_rate_pct
FROM fact_violations v
JOIN dim_lots l ON l.lot_id = v.lot_id
GROUP BY l.lot_name, l.area
HAVING violation_cases >= 50
ORDER BY total_penalty DESC, avg_overstay_minutes DESC;

# Revenue Driver Breakdown
SELECT
  l.lot_name,
  l.area,
  e.vehicle_id AS vehicle_type,
 COALESCE(p.payment_methos_clean, p.payment_method) AS payment_method,
 CASE
    WHEN e.actual_minutes IS NULL THEN 'Unknown'
    WHEN e.actual_minutes <= 60 THEN '0–1 Hour'
    WHEN e.actual_minutes <= 480 THEN '1–8 Hours'
    WHEN e.actual_minutes <= 1440 THEN '8–24 Hours'
    WHEN e.actual_minutes <= 10080 THEN '1–4 Weeks'
    ELSE '1+ Month'
  END AS duration_bucket,
  v.parking_outcome,
  ROUND(SUM(v.final_total_parking_amount), 2) AS revenue
FROM fact_events e
JOIN dim_slots s ON s.slot_id = e.slot_id
JOIN dim_lots  l ON l.lot_id = s.lot_id
LEFT JOIN fact_payments p ON p.event_id = e.event_id
LEFT JOIN fact_violations v ON v.event_id = e.event_id
GROUP BY
  l.lot_name,
  l.area,
  e.vehicle_id,
COALESCE(p.payment_methos_clean, p.payment_method),
CASE
    WHEN e.actual_minutes IS NULL THEN 'Unknown'
    WHEN e.actual_minutes <= 60 THEN '0–1 Hour'
    WHEN e.actual_minutes <= 480 THEN '1–8 Hours'
    WHEN e.actual_minutes <= 1440 THEN '8–24 Hours'
    WHEN e.actual_minutes <= 10080 THEN '1–4 Weeks'
    ELSE '1+ Month'
  END,
v.parking_outcome
ORDER BY revenue DESC
LIMIT 1000;

# Area Performance Matrix
WITH area_capacity AS (
  SELECT l.area, COUNT(*) AS capacity
  FROM dim_slots s
  JOIN dim_lots l ON l.lot_id = s.lot_id
  GROUP BY l.area
),
area_sessions AS (
  SELECT
    l.area,
    AVG(e.actual_minutes) AS avg_minutes
  FROM fact_events e
  JOIN dim_slots s ON s.slot_id = e.slot_id
  JOIN dim_lots l ON l.lot_id = s.lot_id
  WHERE e.actual_minutes IS NOT NULL
  GROUP BY l.area
),
area_revenue AS (
  SELECT
    l.area,
    SUM(v.final_total_parking_amount) AS revenue
  FROM fact_violations v
  JOIN dim_lots l ON l.lot_id = v.lot_id
  GROUP BY l.area
)
SELECT
  c.area,
  ROUND(r.revenue, 2) AS revenue,
  c.capacity,
  ROUND(100 * (s.avg_minutes / 1440.0), 2) AS utilization_pct
FROM area_capacity c
LEFT JOIN area_revenue r ON r.area = c.area
LEFT JOIN area_sessions s ON s.area = c.area
ORDER BY revenue DESC;

# Top 3 Revenue Generating Lots
SELECT
  l.lot_name,
  ROUND(SUM(v.final_total_parking_amount), 2) AS revenue
FROM fact_violations v
JOIN dim_lots l ON l.lot_id = v.lot_id
GROUP BY l.lot_name
ORDER BY revenue DESC
LIMIT 3;

# Average Occupancy %
WITH total_minutes AS (
  SELECT SUM(actual_minutes) AS occupied_minutes
  FROM fact_events
  WHERE actual_minutes IS NOT NULL
),
available_minutes AS (
  SELECT 
    COUNT(*) * 1440 *
    (SELECT COUNT(DISTINCT DATE(entry_time)) FROM fact_events WHERE entry_time IS NOT NULL)
    AS total_available_minutes
  FROM dim_slots
)
SELECT
  ROUND(
    100 * t.occupied_minutes / NULLIF(a.total_available_minutes,0),
    2
  ) AS avg_occupancy_pct
FROM total_minutes t
CROSS JOIN available_minutes a;

# Avg Daily Idle Time per Slot (Hours)
WITH slot_usage AS (
  SELECT
    s.slot_id,
    SUM(e.actual_minutes) / 60.0 AS occupied_hours
  FROM dim_slots s
  LEFT JOIN fact_events e ON e.slot_id = s.slot_id
  GROUP BY s.slot_id
)
SELECT
  ROUND(AVG(24 - (occupied_hours / 
    (SELECT COUNT(DISTINCT DATE(entry_time)) FROM fact_events))), 2
  ) AS avg_daily_idle_hours
FROM slot_usage;

# Long-Term Parking %
SELECT
  ROUND(
    100 * SUM(CASE WHEN actual_minutes > 1440 THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*),0),
    2
  ) AS long_term_parking_pct
FROM fact_events;

# Turnover per Slot
SELECT
  ROUND(
    COUNT(*) / NULLIF((SELECT COUNT(*) FROM dim_slots),0),
    2
  ) AS turnover_per_slot
FROM fact_events;

# Parking Demand Intensity
SELECT
  DAYNAME(entry_time) AS day_name,
  WEEKDAY(entry_time) AS day_no,
  HOUR(entry_time) AS hour_of_day,
  COUNT(*) AS sessions
FROM fact_events
WHERE entry_time IS NOT NULL
GROUP BY day_name, day_no, hour_of_day
ORDER BY day_no, hour_of_day;

# Hourly Occupancy Curve (Avg minutes by hour)
SELECT
  HOUR(entry_time) AS hour_of_day,
  ROUND(AVG(actual_minutes), 2) AS avg_parking_minutes
FROM fact_events
WHERE entry_time IS NOT NULL
GROUP BY hour_of_day
ORDER BY hour_of_day;

# Parking Duration Pattern
SELECT
  CASE
    WHEN actual_minutes <= 60 THEN '0–1 Hour'
    WHEN actual_minutes <= 480 THEN '1–8 Hours'
    WHEN actual_minutes <= 1440 THEN '8–24 Hours'
    ELSE '24+ Hours'
  END AS duration_bucket,
  COUNT(*) AS sessions,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM fact_events
GROUP BY duration_bucket
ORDER BY sessions DESC;

# Parking Utilization by Day Type (Weekday vs Weekend)
SELECT
  CASE
    WHEN WEEKDAY(entry_time) < 5 THEN 'Weekday'
    ELSE 'Weekend'
  END AS day_type,
  ROUND(100 * SUM(actual_minutes) /
    NULLIF(
      COUNT(DISTINCT DATE(entry_time)) *
      1440 *
      (SELECT COUNT(*) FROM dim_slots),
    0),
  2) AS utilization_pct
FROM fact_events
GROUP BY day_type;

# Area Capacity Distribution
SELECT
  l.area,
  COUNT(s.slot_id) AS total_slots
FROM dim_slots s
JOIN dim_lots l ON l.lot_id = s.lot_id
GROUP BY l.area
ORDER BY total_slots DESC;

# Lot-Level Performance Summary
SELECT
  l.lot_name,
  ROUND(AVG(e.actual_minutes), 2) AS avg_actual_parking_minutes,
  ROUND(
    100 * SUM(e.actual_minutes) /
    NULLIF(
      COUNT(DISTINCT DATE(e.entry_time)) *
      1440 *
      COUNT(DISTINCT s.slot_id),
    0),
  2) AS area_utilization_pct,
 ROUND(
    AVG(24 - (e.actual_minutes / 60.0)),
  2) AS avg_daily_idle_time_hours
FROM dim_lots l
JOIN dim_slots s ON s.lot_id = l.lot_id
LEFT JOIN fact_events e ON e.slot_id = s.slot_id
GROUP BY l.lot_name
ORDER BY area_utilization_pct DESC;

# Daily Violation Trend (Mon–Sun)
SELECT
  DAYNAME(e.entry_time) AS day_name,
  WEEKDAY(e.entry_time) AS day_no,
  COUNT(*) AS violation_count
FROM fact_violations v
JOIN fact_events e ON e.event_id = v.event_id
WHERE e.entry_time IS NOT NULL
GROUP BY day_name, day_no
ORDER BY day_no;

# Monthly Violation Revenue Trend (Jan–Dec)
SELECT
  DATE_FORMAT(e.entry_time, '%Y-%m-01') AS month_start,
  ROUND(SUM(v.penalty_amount), 2) AS violation_revenue
FROM fact_violations v
JOIN fact_events e ON e.event_id = v.event_id
WHERE e.entry_time IS NOT NULL
GROUP BY month_start
ORDER BY month_start;

# System Issues vs Customer Violations
SELECT
  CASE
    WHEN v.parking_outcome = 'Timing Discrepancy' THEN 'System Issue'
    ELSE 'Customer Violation'
  END AS issue_type,
  COUNT(*) AS cases,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM fact_violations v
GROUP BY issue_type
ORDER BY cases DESC;

# Lot Risk Matrix
WITH lot_sessions AS (
  SELECT
    l.lot_id,
    COUNT(e.event_id) AS sessions
  FROM dim_lots l
  JOIN dim_slots s ON s.lot_id = l.lot_id
  LEFT JOIN fact_events e ON e.slot_id = s.slot_id
  GROUP BY l.lot_id
),
lot_viol AS (
  SELECT
    v.lot_id,
    COUNT(*) AS violations,
    AVG(v.overstayed_minutes) AS avg_overstay
  FROM fact_violations v
  GROUP BY v.lot_id
)
SELECT
  l.lot_name,
  l.area,
  ROUND(lv.avg_overstay, 2) AS avg_overstay_minutes,
  ROUND( lv.violations / NULLIF(ls.sessions,0), 4) AS violation_rate
FROM dim_lots l
LEFT JOIN lot_sessions ls ON ls.lot_id = l.lot_id
LEFT JOIN lot_viol lv ON lv.lot_id = l.lot_id
WHERE lv.violations IS NOT NULL
ORDER BY violation_rate DESC;

# Violations by Penalty Severity Level
SELECT
  penalty_factor AS severity_level,
  COUNT(*) AS violation_count
FROM fact_violations
WHERE penalty_factor IS NOT NULL
GROUP BY severity_level
ORDER BY severity_level;

# Violation Distribution by Parking Outcome
SELECT
  parking_outcome,
  COUNT(*) AS cases,
  ROUND(SUM(penalty_amount), 2) AS penalty_revenue
FROM fact_violations
GROUP BY parking_outcome
ORDER BY cases DESC;

# Lots exceeding target violation rate (actionable list)
WITH lot_sessions AS (
  SELECT l.lot_id, COUNT(e.event_id) AS sessions
  FROM dim_lots l
  JOIN dim_slots s ON s.lot_id = l.lot_id
  LEFT JOIN fact_events e ON e.slot_id = s.slot_id
  GROUP BY l.lot_id
),
lot_viol AS (
  SELECT lot_id, COUNT(*) AS violations
  FROM fact_violations
  GROUP BY lot_id
)
SELECT
  l.lot_name,
  l.area,
  lv.violations,
  ls.sessions,
  ROUND(lv.violations / NULLIF(ls.sessions,0), 4) AS violation_rate
FROM dim_lots l
JOIN lot_sessions ls ON ls.lot_id = l.lot_id
JOIN lot_viol lv ON lv.lot_id = l.lot_id
WHERE (lv.violations / NULLIF(ls.sessions,0)) > 0.10
ORDER BY violation_rate DESC;

# Month-over-month change in violation revenue
WITH m AS (
  SELECT
    DATE_FORMAT(e.entry_time, '%Y-%m-01') AS month_start,
    SUM(v.penalty_amount) AS viol_rev
  FROM fact_violations v
  JOIN fact_events e ON e.event_id = v.event_id
  GROUP BY month_start
)
SELECT
  month_start,
  ROUND(viol_rev, 2) AS viol_rev,
  ROUND(viol_rev - LAG(viol_rev) OVER (ORDER BY month_start), 2) AS mom_change
FROM m
ORDER BY month_start;

# Simulated Revenue + Uplift Amount
SET @fee_increase_pct = 0.05;   -- 5%
SET @fee_increase_pct = 0.10;   -- 10%
SET @fee_increase_pct = 0.15;   -- 15%

SELECT
  ROUND(SUM(final_total_parking_amount) * (1 + @fee_increase_pct), 2) AS simulated_revenue
FROM fact_violations;
SELECT
  ROUND(SUM(final_total_parking_amount), 2) AS base_revenue,
  ROUND(SUM(final_total_parking_amount) * (1 + @fee_increase_pct), 2) AS simulated_revenue,
  ROUND(SUM(final_total_parking_amount) * @fee_increase_pct, 2) AS uplift_amount,
  ROUND(100 * @fee_increase_pct, 2) AS uplift_pct
FROM fact_violations;

# Revenue Uplift Amount by Parking Outcome
SELECT
  v.parking_outcome,
  ROUND(SUM(v.final_total_parking_amount) * @fee_increase_pct, 2) AS uplift_amount
FROM fact_violations v
GROUP BY v.parking_outcome
ORDER BY uplift_amount DESC;

# Projected Revenue (Next 3 Months)
WITH monthly AS (
  SELECT
    DATE_FORMAT(e.entry_time, '%Y-%m-01') AS month_start,
    SUM(v.final_total_parking_amount) AS revenue
  FROM fact_events e
  JOIN fact_violations v ON v.event_id = e.event_id
  WHERE e.entry_time IS NOT NULL
  GROUP BY month_start
),
last3 AS (
  SELECT revenue
  FROM monthly
  ORDER BY month_start DESC
  LIMIT 3
)
SELECT
  ROUND( (SELECT AVG(revenue) FROM last3) * 3 * (1 + @fee_increase_pct), 2) AS projected_revenue_next_3_months;

# Projected Demand Growth %
WITH monthly_sessions AS (
  SELECT DATE_FORMAT(entry_time, '%Y-%m-01') AS month_start, COUNT(*) AS sessions
  FROM fact_events
  WHERE entry_time IS NOT NULL
  GROUP BY month_start
),
ranked AS (
  SELECT month_start, sessions,
         ROW_NUMBER() OVER (ORDER BY month_start DESC) AS rn
  FROM monthly_sessions
),
calc AS (
  SELECT
    SUM(CASE WHEN rn BETWEEN 1 AND 3 THEN sessions END) AS last3,
    SUM(CASE WHEN rn BETWEEN 4 AND 6 THEN sessions END) AS prev3
  FROM ranked
)
SELECT
  last3, prev3,
  ROUND(100 * (last3 - prev3) / NULLIF(prev3, 0), 2) AS demand_growth_pct,
  CASE
    WHEN last3 >= prev3 THEN 'Growth'
    ELSE 'Decline'
  END AS trend_label
FROM calc;

# Risk Level (Acceptable/High)
WITH totals AS (
  SELECT
    SUM(final_total_parking_amount) AS total_rev,
    SUM(penalty_amount) AS penalty_rev
  FROM fact_violations
)
SELECT
  ROUND(100 * penalty_rev / NULLIF(total_rev,0), 2) AS penalty_share_pct,
  CASE
    WHEN (penalty_rev / NULLIF(total_rev,0)) >= 0.85 THEN 'High Risk'
    WHEN (penalty_rev / NULLIF(total_rev,0)) >= 0.70 THEN 'Moderate Risk'
    ELSE 'Acceptable Risk'
  END AS risk_level
FROM totals;

# Uplift by lot (who benefits most from price increase)
SELECT
  l.lot_name,
  ROUND(SUM(v.final_total_parking_amount), 2) AS current_revenue,
  ROUND(SUM(v.final_total_parking_amount) * @fee_increase_pct, 2) AS uplift_amount,
  ROUND(SUM(v.final_total_parking_amount) * (1 + @fee_increase_pct), 2) AS simulated_revenue
FROM fact_violations v
JOIN dim_lots l ON l.lot_id = v.lot_id
GROUP BY l.lot_name
ORDER BY uplift_amount DESC;

# “Optimization recommendation” query: find outcomes with highest incremental value
SELECT
  parking_outcome,
  ROUND(SUM(final_total_parking_amount), 2) AS revenue,
  ROUND(SUM(final_total_parking_amount) * @fee_increase_pct, 2) AS incremental_value
FROM fact_violations
GROUP BY parking_outcome
ORDER BY incremental_value DESC;

# Demand sensitivity proxy (price vs sessions)
WITH monthly AS (
  SELECT
    DATE_FORMAT(e.entry_time, '%Y-%m-01') AS month_start,
    COUNT(DISTINCT e.event_id) AS sessions,
    SUM(v.final_total_parking_amount) AS revenue
  FROM fact_events e
  JOIN fact_violations v ON v.event_id = e.event_id
  GROUP BY month_start
)
SELECT
  month_start,
  sessions,
  ROUND(revenue, 2) AS revenue,
  ROUND(revenue / NULLIF(sessions,0), 2) AS revenue_per_session
FROM monthly
ORDER BY month_start;


