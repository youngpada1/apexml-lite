variable "snowflake_account" {
  description = "Snowflake account identifier (without organization prefix)"
  type        = string
}

variable "snowflake_organization" {
  description = "Snowflake organization name"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake username for Terraform"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake password for Terraform"
  type        = string
  sensitive   = true
}

variable "snowflake_warehouse" {
  description = "Snowflake warehouse name"
  type        = string
  default     = "APEXML_WH"
}

variable "snowflake_database" {
  description = "Snowflake database name"
  type        = string
  default     = "APEXML_DB"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "Environment must be dev or prod."
  }
}
