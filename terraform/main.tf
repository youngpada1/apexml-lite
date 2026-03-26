# ─────────────────────────────────────────────
# Warehouse
# ─────────────────────────────────────────────
resource "snowflake_warehouse" "apexml_wh" {
  name           = var.snowflake_warehouse
  warehouse_size = "X-SMALL"
  auto_suspend   = 60
  auto_resume    = true
  comment        = "ApexML-Lite compute warehouse"
}

# ─────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────
resource "snowflake_database" "apexml_db" {
  name    = var.snowflake_database
  comment = "ApexML-Lite F1 analytics database"
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

resource "snowflake_schema" "analytics" {
  database = snowflake_database.apexml_db.name
  name     = "ANALYTICS"
  comment  = "dbt marts — facts, dimensions, KPIs for Streamlit and Cortex"
}
