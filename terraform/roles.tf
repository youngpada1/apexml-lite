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
# Schema grants — WRITER → RAW
# ─────────────────────────────────────────────
resource "snowflake_grant_privileges_to_account_role" "writer_raw_schema" {
  account_role_name = snowflake_account_role.writer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE STAGE"]
  on_schema {
    schema_name = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.raw.name}\""
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

resource "snowflake_grant_privileges_to_account_role" "transformer_prod_schema" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  on_schema {
    schema_name = "\"${snowflake_database.apexml_db.name}\".\"${snowflake_schema.prod.name}\""
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
