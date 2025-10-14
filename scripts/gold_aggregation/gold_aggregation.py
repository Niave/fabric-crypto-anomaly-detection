import os 
import argparse
import logging
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, flatten, lower, max as sf_max ,when_matched, when_not_matched, call_table_function, lit, when, count , avg, datediff
from snowflake.snowpark.table import  MergeResult 
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")



SILVER = "SILVER"
GOLD = "GOLD"
GOLD_STAGE = "GOLD_STAGING"

SILVER_EVENTS = f"{SILVER}.EVENTS_CLEANED"
SILVER_SESSIONS = f"{SILVER}.SESSION_EVENTS"

USER_METRICS_TABLE = f"{GOLD}.USER_METRICS"
SESSION_METRICS_TABLE = f"{GOLD}.SESSION_METRICS"
PRODUCT_METRICS_TABLE = f"{GOLD}.PRODUCT_METRICS"

USER_STAGE = f"{GOLD_STAGE}.USER_METRICS_STAGE"
SESSION_STAGE = f"{GOLD_STAGE}.SESSION_METRICS_STAGE"
PRODUCT_STAGE = f"{GOLD_STAGE}.PRODUCT_METRICS_STAGE"


def ensure_gold_tables(session: Session) -> None:
    logging.info("Ensuring Gold layer tables exist...")
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {USER_METRICS_TABLE} (
            user_id STRING,
            total_events INT,
            num_purchases INT,
            num_clicks INT,
            conversion_rate FLOAT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()

    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {SESSION_METRICS_TABLE} (
            session_id STRING,
            user_id STRING,
            session_duration_minutes FLOAT,
            num_events INT,
            is_bounce BOOLEAN,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()

    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {PRODUCT_METRICS_TABLE} (
            product_id STRING,
            num_views INT,
            num_add_to_cart INT,
            num_purchases INT,
            click_to_purchase_rate FLOAT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()



def get_last_ingested_at(session: Session, table_name: str, timestamp_col: str = "ingested_at"):
    """Return latest ingested_at timestamp or default."""
    try:
        max_ts = session.table(table_name).select(sf_max(col(timestamp_col))).collect()[0][0]
        if max_ts is None:
            logging.info(f"No data in {table_name}, performing full load.")
            return "1970-01-01 00:00:00"
        logging.info(f"Last ingested record in {table_name}: {max_ts}")
        return max_ts
    except Exception as e:
        logging.warning(f"Could not read {timestamp_col} from {table_name}: {e}")
        return "1970-01-01 00:00:00"



def compute_user_metrics(session: Session) -> None:
    logging.info("Starting USER_METRICS incremental load...")
    last_ingested_at = get_last_ingested_at(session, USER_METRICS_TABLE)
    user_metric_df = session.table(SILVER_EVENTS).filter(col("ingested_at") > last_ingested_at)

    if user_metric_df.count() == 0:
        logging.info("No new user event data to process.")
        return
    
    max_ingested_at = user_metric_df.select(sf_max(col("ingested_at"))).collect()[0][0]

    user_metrics = (
        user_metric_df.group_by("user_id")
        .agg(
            count("*").alias("total_events"),
            count(when(col("event_type") == "purchase", True)).alias("num_purchases"),
            count(when(col("event_type").isin("view_product", "add_to_cart", "remove_from_cart"), True)).alias("num_clicks")
        )
        .with_column(
            "conversion_rate",
            when(col("num_clicks") == 0, lit(0)).otherwise(col("num_purchases") / col("num_clicks"))
        )
        .with_column("ingested_at", lit(max_ingested_at))
    )


    user_metrics.write.save_as_table(USER_STAGE, mode= "overwrite")

    target = session.table(USER_METRICS_TABLE).alias("target")
    staging = session.table(USER_STAGE).alias("staging")

    merge_result: MergeResult = target.merge(
        staging,
        target["user_id"] == staging["user_id"],
        [
            when_matched().update({
                "total_events": staging["total_events"],
                "num_purchases": staging["num_purchases"],
                "num_clicks": staging["num_clicks"],
                "conversion_rate": staging["conversion_rate"],
                "ingested_at": staging["ingested_at"]
            }),
            when_not_matched().insert({
                "user_id": staging["user_id"],
                "total_events": staging["total_events"],
                "num_purchases": staging["num_purchases"],
                "num_clicks": staging["num_clicks"],
                "conversion_rate": staging["conversion_rate"],
                "ingested_at": staging["ingested_at"]
            })
        ]
    )
    logging.info(f"User metrics merged: {merge_result.rows_inserted} inserted, {merge_result.rows_updated} updated")


def compute_session_metrics(session: Session) -> None:
    logging.info("Starting SESSION_METRICS incremental load...")
    last_ingested_at = get_last_ingested_at(session, SESSION_METRICS_TABLE)
    session_metrics_df = session.table(SILVER_SESSIONS).filter(col("ingested_at") > last_ingested_at)

    if session_metrics_df.count() == 0:
        logging.info("No new session data to process.")
        return
    max_ingested_at = session_metrics_df.select(sf_max(col("ingested_at"))).collect()[0][0]

    session_metrics = (
        session_metrics_df.group_by("session_id", "user_id")
        .agg(
            count("*").alias("num_events"),
            avg(datediff("minute", col("start_time"), col("end_time"))).alias("session_duration_minutes")
        )
        .with_column(
            "is_bounce",
            when(col("num_events") == 1, lit(True)).otherwise(lit(False))
        )
        .with_column(
            "ingested_at",
            lit(max_ingested_at)
        )
    )

    session_metrics.write.save_as_table(SESSION_STAGE, mode="overwrite")

    target = session.table(SESSION_METRICS_TABLE).alias("target")
    staging = session.table(SESSION_STAGE).alias("staging")

    merge_result: MergeResult = target.merge(
        staging,
        target["session_id"] == staging["session_id"],
        [
            when_matched().update({
                "user_id": staging["user_id"],
                "session_duration_minutes": staging["session_duration_minutes"],
                "num_events": staging["num_events"],
                "is_bounce": staging["is_bounce"],
                "ingested_at": staging["ingested_at"]
            }),
            when_not_matched().insert({
                "session_id": staging["session_id"],
                "user_id": staging["user_id"],
                "session_duration_minutes": staging["session_duration_minutes"],
                "num_events": staging["num_events"],
                "is_bounce": staging["is_bounce"],
                "ingested_at": staging["ingested_at"]
            })
        ]
    )
    logging.info(f"Session metrics merged: {merge_result.rows_inserted} inserted, {merge_result.rows_updated} updated.")

def compute_product_metrics(session: Session) -> None:
    logging.info("Starting PRODUCT_METRICS incremental load...")
    last_ingested_at = get_last_ingested_at(session, PRODUCT_METRICS_TABLE)
    product_metrics_df = session.table(SILVER_EVENTS).filter(col("ingested_at") > last_ingested_at)

    if product_metrics_df.count() == 0:
        logging.info("No new product event data to process.")
        return
    
    max_ingested_at = product_metrics_df.select(sf_max(col("ingested_at"))).collect()[0][0]

    product_metrics = (
        product_metrics_df.group_by("product_id")
        .agg(
            count(when(col("event_type") == "view_product", True)).alias("num_views"),
            count(when(col("event_type") == "add_to_cart", True)).alias("num_add_to_cart"),
            count(when(col("event_type") == "purchase", True)).alias("num_purchases")
        )
        .with_column(
            "click_to_purchase_rate",
            when((col("num_views") + col("num_add_to_cart")) == 0, lit(0))
            .otherwise(col("num_purchases") / (col("num_views") + col("num_add_to_cart")))
        )
        .with_column("ingested_at", lit(max_ingested_at))
    )
    product_metrics.write.save_as_table(PRODUCT_STAGE, mode="overwrite")
    target = session.table(PRODUCT_METRICS_TABLE).alias("target")
    staging = session.table(PRODUCT_STAGE).alias("staging")

    merge_result: MergeResult = target.merge(
        staging,
        target["product_id"] == staging["product_id"],
        [
            when_matched().update({
                "num_views": staging["num_views"],
                "num_add_to_cart": staging["num_add_to_cart"],
                "num_purchases": staging["num_purchases"],
                "click_to_purchase_rate": staging["click_to_purchase_rate"],
                "ingested_at": staging["ingested_at"]
            }),
            when_not_matched().insert({
                "product_id": staging["product_id"],
                "num_views": staging["num_views"],
                "num_add_to_cart": staging["num_add_to_cart"],
                "num_purchases": staging["num_purchases"],
                "click_to_purchase_rate": staging["click_to_purchase_rate"],
                "ingested_at": staging["ingested_at"]
            })
        ]
    )
    logging.info(f"Product metrics merged: {merge_result.rows_inserted} inserted, {merge_result.rows_updated} updated.")



def main(step: str, env_path: str):

    # Check if file exists
    if not os.path.exists(env_path):
        raise FileNotFoundError(f".env file not found at path: {env_path}")
    #Default Configs
    load_dotenv(dotenv_path=env_path) #Load credentials found in .env file

    #Connection credentials
    connection_params={
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }
    # Check that all required environment variables are set
    missing = [k for k,v in connection_params.items() if not v]
    if missing:
        raise ValueError(f"Missing enviroment variables: {','.join(missing)}")

    session = Session.builder.configs(connection_params).create()
    ensure_gold_tables(session)
    try:
        if step in ["all","users"]:
            compute_user_metrics(session)
        if step in ["all","sessions"]:
            compute_session_metrics(session)    
        if step in ["all","products"]:
            compute_product_metrics(session)
    finally:
        session.close()
        logging.info("Snowpark session closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", choices=["all", "users", "sessions", "products"], default="all")
    parser.add_argument("--env", default=".env", help="Path to .env file")
    args = parser.parse_args()
    main(args.step, args.env)