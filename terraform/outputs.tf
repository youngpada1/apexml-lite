output "warehouse_name" {
  description = "Snowflake warehouse name"
  value       = snowflake_warehouse.apexml_wh.name
}

output "database_name" {
  description = "Snowflake database name"
  value       = snowflake_database.apexml_db.name
}

output "schema_raw" {
  description = "RAW schema name"
  value       = snowflake_schema.raw.name
}

output "schema_staging" {
  description = "STAGING schema name"
  value       = snowflake_schema.staging.name
}

output "schema_prod" {
  description = "PROD schema name"
  value       = snowflake_schema.prod.name
}

output "role_writer" {
  description = "Writer role name"
  value       = snowflake_account_role.writer.name
}

output "role_transformer" {
  description = "Transformer role name"
  value       = snowflake_account_role.transformer.name
}

output "role_reader" {
  description = "Reader role name"
  value       = snowflake_account_role.reader.name
}
