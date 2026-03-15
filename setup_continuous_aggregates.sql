-- Continuous aggregates for all measurement tables.
--
-- Strategy:
--   - cumulative sources (power, gas): store counter_agg per hour; queries
--     roll up with rollup() and apply interpolated_delta at query time.
--   - direct sources (water, generation, temperatures): store SUM/MAX per hour;
--     queries re-aggregate to the requested bucket size.
--
-- CAs default to materialized_only=false (real-time), so non-materialized
-- recent chunks are included transparently — no raw-table fallback needed.
--
-- Run once. The CALL refresh_... lines at the bottom backfill history and
-- may take several minutes on first run.

-- ============================================================
-- Power: stores counter_agg for both metering directions
-- ============================================================
CREATE MATERIALIZED VIEW power_hourly_ca
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', created, 'Europe/Amsterdam') AS bucket,
  counter_agg(created, cumulative_from_network_wh) AS from_network_agg,
  counter_agg(created, cumulative_to_network_wh)   AS to_network_agg
FROM power
GROUP BY bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('power_hourly_ca',
  start_offset      => INTERVAL '3 hours',
  end_offset        => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour');

-- ============================================================
-- Gas: single cumulative counter
-- ============================================================
CREATE MATERIALIZED VIEW gas_hourly_ca
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', created, 'Europe/Amsterdam') AS bucket,
  counter_agg(created, cumulative_total_dm3) AS total_agg
FROM gas
GROUP BY bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('gas_hourly_ca',
  start_offset      => INTERVAL '3 hours',
  end_offset        => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour');

-- ============================================================
-- Water: direct usage, refresh frequently (ticks come in real-time)
-- ============================================================
CREATE MATERIALIZED VIEW water_hourly_ca
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', created, 'Europe/Amsterdam') AS bucket,
  SUM(usage_dl) AS usage_dl
FROM water
GROUP BY bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('water_hourly_ca',
  start_offset      => INTERVAL '3 hours',
  end_offset        => INTERVAL '30 minutes',
  schedule_interval => INTERVAL '30 minutes');

-- ============================================================
-- Generation: direct usage (solar Wh per reading)
-- ============================================================
CREATE MATERIALIZED VIEW generation_hourly_ca
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', created, 'Europe/Amsterdam') AS bucket,
  SUM(generation_wh) AS generation_wh
FROM generation
GROUP BY bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('generation_hourly_ca',
  start_offset      => INTERVAL '3 hours',
  end_offset        => INTERVAL '30 minutes',
  schedule_interval => INTERVAL '30 minutes');

-- ============================================================
-- Temperatures: MAX per room per hour
-- ============================================================
CREATE MATERIALIZED VIEW temperatures_hourly_ca
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', created, 'Europe/Amsterdam') AS bucket,
  MAX(huiskamer) AS huiskamer,
  MAX(tuinkamer) AS tuinkamer,
  MAX(zolder)    AS zolder
FROM temperatures
GROUP BY bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy('temperatures_hourly_ca',
  start_offset      => INTERVAL '3 hours',
  end_offset        => INTERVAL '30 minutes',
  schedule_interval => INTERVAL '30 minutes');

-- ============================================================
-- Grant SELECT to the API user
-- ============================================================
GRANT SELECT ON power_hourly_ca        TO web_api;
GRANT SELECT ON gas_hourly_ca          TO web_api;
GRANT SELECT ON water_hourly_ca        TO web_api;
GRANT SELECT ON generation_hourly_ca   TO web_api;
GRANT SELECT ON temperatures_hourly_ca TO web_api;

-- ============================================================
-- Backfill all CAs with historical data (runs once, may be slow)
-- ============================================================
CALL refresh_continuous_aggregate('power_hourly_ca',        NULL, NULL);
CALL refresh_continuous_aggregate('gas_hourly_ca',          NULL, NULL);
CALL refresh_continuous_aggregate('water_hourly_ca',        NULL, NULL);
CALL refresh_continuous_aggregate('generation_hourly_ca',   NULL, NULL);
CALL refresh_continuous_aggregate('temperatures_hourly_ca', NULL, NULL);
