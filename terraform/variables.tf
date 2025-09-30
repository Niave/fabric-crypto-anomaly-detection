variable "snowflake_username" {
  type = string
}

variable "snowflake_password" {
  type = string
  sensitive = true
}

variable "snowflake_account" {
  type = string
}

variable "snowflake_region" {
  type = string
  default = "azure-west-europe"  # Adjust based on your account
}


