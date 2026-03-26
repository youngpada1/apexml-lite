terraform {
  required_version = ">= 1.6.0"

  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 1.0"
    }
  }

  cloud {
    organization = "apexml-lite"

    workspaces {
      name = "apexml-lite"
    }
  }
}

provider "snowflake" {
  account_name      = var.snowflake_account
  organization_name = var.snowflake_organization
  user              = var.snowflake_user
  private_key       = var.snowflake_private_key
  role              = "ACCOUNTADMIN"
}
