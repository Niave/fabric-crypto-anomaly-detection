
resource "snowflake_database" "product_analytics_db" {
  name = "PRODUCT_ANALYTICS"
  comment = "Database for product analytics pipeline"
}

resource "snowflake_schema" "bronze" {
  name     = "BRONZE"
  database = snowflake_database.product_analytics_db.name
}

resource "snowflake_schema" "silver" {
  name     = "SILVER"
  database = snowflake_database.product_analytics_db.name
}
resource "snowflake_schema" "silver_staging" {
  name     = "SILVER_STAGING"
  database = snowflake_database.product_analytics_db.name
  comment  = "Staging schema for the Silver layer"
}

resource "snowflake_schema" "gold" {
  name     = "GOLD"
  database = snowflake_database.product_analytics_db.name
}

resource "snowflake_schema" "gold_staging" {
  name     = "GOLD_STAGING"
  database = snowflake_database.product_analytics_db.name
  comment  = "Staging schema for the Gold layer"
}

resource "snowflake_warehouse" "analytics_wh" {
  name            = "ANALYTICS_WH"
  warehouse_size  = "XSMALL"
  auto_suspend    = 60
  auto_resume     = true
  initially_suspended = true
  comment         = "Warehouse for product analytics"
}
