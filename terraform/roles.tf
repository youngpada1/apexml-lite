# ─────────────────────────────────────────────
# Roles
# ─────────────────────────────────────────────
resource "snowflake_account_role" "writer" {
  name    = "APEXML_WRITER"
  comment = "Ingestion role — writes raw OpenF1 data to RAW schema"
}

resource "snowflake_account_role" "transformer" {
  name    = "APEXML_TRANSFORMER"
  comment = "dbt role — reads RAW, writes STAGING and PROD"
}

resource "snowflake_account_role" "reader" {
  name    = "APEXML_READER"
  comment = "Read-only role — Streamlit and Cortex AI access to PROD"
}

# ─────────────────────────────────────────────
# Warehouse grants
# ─────────────────────────────────────────────
resource "snowflake_grant_privileges_to_account_role" "writer_warehouse" {
  account_role_name = snowflake_account_role.writer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.apexml_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_warehouse" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.apexml_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_warehouse" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.apexml_wh.name
  }
}

# ─────────────────────────────────────────────
# Database grants
# ─────────────────────────────────────────────
resource "snowflake_grant_privileges_to_account_role" "writer_database" {
  account_role_name = snowflake_account_role.writer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.apexml_db.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_database" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.apexml_db.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_database" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.apexml_db.name
  }
}

# ─────────────────────────────────────────────
# Schema grants — WRITER → RAW + STAGING + PROD (USAGE)
# ─────────────────────────────────────────────
resource "snowflake_grant_privileges_to_account_role" "writer_raw_schema" {
  account_role_name = snowflake_account_role.writer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE STAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.raw.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "writer_raw_create_table" {
  account_role_name = snowflake_account_role.writer.name
  privileges        = ["CREATE TABLE"]
  on_schema {
    schema_name = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.raw.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "writer_staging_schema" {
  account_role_name = snowflake_account_role.writer.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.staging.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "writer_prod_schema" {
  account_role_name = snowflake_account_role.writer.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.prod.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "writer_raw_tables" {
  account_role_name = snowflake_account_role.writer.name
  privileges        = ["INSERT", "UPDATE", "SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.raw.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "writer_raw_tables_future" {
  account_role_name = snowflake_account_role.writer.name
  privileges        = ["INSERT", "UPDATE", "SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.raw.name}\""
    }
  }
}

# ─────────────────────────────────────────────
# Schema grants — TRANSFORMER → RAW (read) + STAGING + PROD (write)
# ─────────────────────────────────────────────
resource "snowflake_grant_privileges_to_account_role" "transformer_raw_schema" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.raw.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_tables" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.raw.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_raw_tables_future" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.raw.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_database_create_schema" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.apexml_db.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_staging_schema" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  on_schema {
    schema_name = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.staging.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_staging_tables" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.staging.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_staging_tables_future" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.staging.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_staging_views_future" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.staging.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_prod_schema" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  on_schema {
    schema_name = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.prod.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_prod_tables" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.prod.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_prod_tables_future" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.prod.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_prod_views_future" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.prod.name}\""
    }
  }
}

# ─────────────────────────────────────────────
# Schema grants — READER → PROD (read-only)
# ─────────────────────────────────────────────
resource "snowflake_grant_privileges_to_account_role" "reader_prod_schema" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.prod.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_prod_tables" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.prod.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_prod_tables_future" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.prod.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_prod_views" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["SELECT"]
  on_schema_object {
    all {
      object_type_plural = "VIEWS"
      in_schema          = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.prod.name}\""
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_prod_views_future" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = "VIEWS"
      in_schema          = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.prod.name}\""
    }
  }
}

# ─────────────────────────────────────────────
# ML Forecast stream grant — TRANSFORMER (read stream for task condition)
# ─────────────────────────────────────────────
resource "snowflake_execute" "transformer_fct_results_stream" {
  execute = "GRANT SELECT ON STREAM APEXML_DB.PROD.FCT_RESULTS_STREAM TO ROLE APEXML_TRANSFORMER"
  revert  = "REVOKE SELECT ON STREAM APEXML_DB.PROD.FCT_RESULTS_STREAM FROM ROLE APEXML_TRANSFORMER"
}

# ─────────────────────────────────────────────
# ML Forecast model grant — READER (call only, no create)
# ─────────────────────────────────────────────
resource "snowflake_execute" "reader_forecast_model" {
  execute = "GRANT USAGE ON MODEL APEXML_DB.PROD.FORECAST_MODEL_CORTEX TO ROLE APEXML_READER"
  revert  = "REVOKE USAGE ON MODEL APEXML_DB.PROD.FORECAST_MODEL_CORTEX FROM ROLE APEXML_READER"
}

# ─────────────────────────────────────────────
# Cortex Stage + AI grants — READER
# ─────────────────────────────────────────────
resource "snowflake_grant_privileges_to_account_role" "reader_cortex_stage" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["READ"]
  on_schema_object {
    object_type = "STAGE"
    object_name = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.prod.name}\".\"${snowflake_stage.cortex_stage.name}\""
  }
}

resource "snowflake_execute" "reader_cortex_user" {
  execute = "GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE ${snowflake_account_role.reader.name}"
  revert  = "REVOKE DATABASE ROLE SNOWFLAKE.CORTEX_USER FROM ROLE ${snowflake_account_role.reader.name}"
}

# ─────────────────────────────────────────────
# Grant roles to SYSADMIN (so admin can switch)
# ─────────────────────────────────────────────
resource "snowflake_grant_account_role" "writer_to_sysadmin" {
  role_name        = snowflake_account_role.writer.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "transformer_to_sysadmin" {
  role_name        = snowflake_account_role.transformer.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "reader_to_sysadmin" {
  role_name        = snowflake_account_role.reader.name
  parent_role_name = "SYSADMIN"
}
