# ─────────────────────────────────────────────
# Resource Monitor
# ─────────────────────────────────────────────
resource "snowflake_resource_monitor" "apexml_monitor" {
  name                    = "APEXML_MONITOR"
  credit_quota            = 50
  frequency               = "MONTHLY"
  start_timestamp         = "IMMEDIATELY"
  notify_triggers           = [75]
  suspend_trigger           = 95
  suspend_immediate_trigger = 100
}

# ─────────────────────────────────────────────
# Warehouse
# ─────────────────────────────────────────────
resource "snowflake_warehouse" "apexml_wh" {
  name             = var.snowflake_warehouse
  warehouse_size   = "X-SMALL"
  auto_suspend     = 60
  auto_resume      = true
  resource_monitor = snowflake_resource_monitor.apexml_monitor.name
  comment          = "ApexML-Lite compute warehouse"
}

# ─────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────
resource "snowflake_database" "apexml_db" {
  name    = var.snowflake_database
  comment = "ApexML-Lite F1 prod database"
}

# ─────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────
resource "snowflake_schema" "raw" {
  database = snowflake_database.apexml_db.name
  name     = "RAW"
  comment  = "Landing zone — raw data from OpenF1 API"
}

resource "snowflake_schema" "staging" {
  database = snowflake_database.apexml_db.name
  name     = "STAGING"
  comment  = "dbt staging — cleaned and typed models"
}

resource "snowflake_schema" "prod" {
  database = snowflake_database.apexml_db.name
  name     = "PROD"
  comment  = "dbt marts — facts, dimensions, KPIs for Streamlit and Cortex"
}

# ─────────────────────────────────────────────
# Cortex Stage
# ─────────────────────────────────────────────
resource "snowflake_stage" "cortex_stage" {
  database = snowflake_database.apexml_db.name
  schema   = snowflake_schema.prod.name
  name     = "CORTEX_STAGE"
  comment  = "Hosts Cortex Analyst semantic model YAML"
}

# ─────────────────────────────────────────────
# ML Forecast — initial model creation
# Run once by APEXML_TRANSFORMER; Task retrains it weekly
# ─────────────────────────────────────────────
resource "snowflake_execute" "forecast_model_create" {
  execute = <<-SQL
    CREATE OR REPLACE SNOWFLAKE.ML.FORECAST APEXML_DB.PROD.FORECAST_MODEL_CORTEX(
      INPUT_DATA => SYSTEM$QUERY_REFERENCE($$
        SELECT
          s.session_start_at::date AS ts,
          r.driver_name            AS series_id,
          SUM(r.points) OVER (
            PARTITION BY r.driver_number
            ORDER BY s.session_start_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS y
        FROM APEXML_DB.PROD.FCT_SESSION_RESULTS r
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON r.session_key = s.session_key
        WHERE s.session_type = 'Race'
          AND s.session_name = 'Race'
          AND s.year = YEAR(CURRENT_DATE())
          AND s.session_start_at < CURRENT_TIMESTAMP()
          AND r.points IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY r.driver_number, s.session_key ORDER BY s.session_start_at
        ) = 1
        ORDER BY r.driver_name, s.session_start_at
      $$),
      SERIES_COLNAME    => 'SERIES_ID',
      TIMESTAMP_COLNAME => 'TS',
      TARGET_COLNAME    => 'Y'
    )
  SQL
  revert = "DROP MODEL IF EXISTS APEXML_DB.PROD.FORECAST_MODEL_CORTEX"
}

# ─────────────────────────────────────────────
# ML Forecast — stream on FCT_SESSION_RESULTS
# Tracks new rows so the task only fires when new race data lands
# ─────────────────────────────────────────────
resource "snowflake_execute" "fct_results_stream" {
  execute = "CREATE OR REPLACE STREAM APEXML_DB.PROD.FCT_RESULTS_STREAM ON TABLE APEXML_DB.PROD.FCT_SESSION_RESULTS APPEND_ONLY = TRUE"
  revert  = "DROP STREAM IF EXISTS APEXML_DB.PROD.FCT_RESULTS_STREAM"
}

# ─────────────────────────────────────────────
# ML Forecast — retrain task
# Polls Monday 08:00 UTC (after race weekend ingest)
# WHEN condition: only executes if stream has unconsumed rows — free no-op otherwise
# ─────────────────────────────────────────────
resource "snowflake_execute" "forecast_training_task" {
  execute = <<-SQL
    CREATE OR REPLACE TASK APEXML_DB.PROD.FORECAST_TRAINING_CORTEX
      WAREHOUSE = '${var.snowflake_warehouse}'
      SCHEDULE  = 'USING CRON 0 8 * * MON UTC'
      WHEN SYSTEM$STREAM_HAS_DATA('APEXML_DB.PROD.FCT_RESULTS_STREAM')
      COMMENT   = 'Retrains FORECAST_MODEL_CORTEX only when new race data lands'
    AS
    CREATE OR REPLACE SNOWFLAKE.ML.FORECAST APEXML_DB.PROD.FORECAST_MODEL_CORTEX(
      INPUT_DATA => SYSTEM$QUERY_REFERENCE($$
        SELECT
          s.session_start_at::date AS ts,
          r.driver_name            AS series_id,
          SUM(r.points) OVER (
            PARTITION BY r.driver_number
            ORDER BY s.session_start_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS y
        FROM APEXML_DB.PROD.FCT_SESSION_RESULTS r
        JOIN APEXML_DB.PROD.DIM_SESSIONS s ON r.session_key = s.session_key
        WHERE s.session_type = 'Race'
          AND s.session_name = 'Race'
          AND s.year = YEAR(CURRENT_DATE())
          AND s.session_start_at < CURRENT_TIMESTAMP()
          AND r.points IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY r.driver_number, s.session_key ORDER BY s.session_start_at
        ) = 1
        ORDER BY r.driver_name, s.session_start_at
      $$),
      SERIES_COLNAME    => 'SERIES_ID',
      TIMESTAMP_COLNAME => 'TS',
      TARGET_COLNAME    => 'Y'
    );
  SQL
  revert     = "DROP TASK IF EXISTS APEXML_DB.PROD.FORECAST_TRAINING_CORTEX"
  depends_on = [snowflake_execute.forecast_model_create, snowflake_execute.fct_results_stream]
}

resource "snowflake_execute" "forecast_training_task_resume" {
  execute    = "ALTER TASK APEXML_DB.PROD.FORECAST_TRAINING_CORTEX RESUME"
  revert     = "ALTER TASK APEXML_DB.PROD.FORECAST_TRAINING_CORTEX SUSPEND"
  depends_on = [snowflake_execute.forecast_training_task]
}
