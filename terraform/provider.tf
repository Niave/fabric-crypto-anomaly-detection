terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.70"  # Use latest or pin to tested version
    }
  }
}

provider "snowflake" {
  user = var.snowflake_username
  password = var.snowflake_password
  account = var.snowflake_account
  role = "SYSADMIN"  # Adjust as needed
}

