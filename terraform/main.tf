# ─────────────────────────────────────────────
# Resource Monitor
# ─────────────────────────────────────────────
resource "snowflake_resource_monitor" "apexml_monitor" {
  name                    = "APEXML_MONITOR"
  credit_quota            = 25
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
