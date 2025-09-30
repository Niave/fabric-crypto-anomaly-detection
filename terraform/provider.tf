terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.70"  # Use latest or pin to tested version
    }
  }
}

provider "snowflake" {
  username = var.snowflake_username
  password = var.snowflake_password
  account  = var.snowflake_account
  role     = "SYSADMIN"  # Adjust as needed
  region   = var.snowflake_region  # Optional if not multi-region
}

