import os 
import argparse
import logging
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, flatten, lower, max as sf_max ,when_matched, when_not_matched, call_table_function
from snowflake.snowpark.table import  MergeResult 
from dotenv import load_dotenv



logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")



BRONZE_SCHEMA = "BRONZE"
SILVER_SCHEMA = "SILVER"
STAGING_SCHEMA = f"{SILVER_SCHEMA}_STAGING"

EVENTS_TABLE = f"{BRONZE_SCHEMA}.EVENTS"
SESSIONS_TABLE = f"{BRONZE_SCHEMA}.SESSIONS"
EVENTS_SILVER_TABLE = f"{SILVER_SCHEMA}.EVENTS_CLEANED"
SESSIONS_SILVER_TABLE = f"{SILVER_SCHEMA}.SESSION_EVENTS"
EVENTS_STAGING_TABLE = f"{STAGING_SCHEMA}.EVENTS_STAGE"
SESSIONS_STAGING_TABLE = f"{STAGING_SCHEMA}.SESSIONS_STAGE"


def ensure_tables(session: Session) -> None:
    logging.info("Ensuring silver tables exist...")

    create_events_sql = f"""
    CREATE TABLE IF NOT EXISTS {EVENTS_SILVER_TABLE} (
        event_id INT,
        user_id STRING,
        event_type STRING,
        product_id STRING,
        timestamp TIMESTAMP,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """

    create_sessions_sql = f"""
    CREATE TABLE IF NOT EXISTS {SESSIONS_SILVER_TABLE} (
        session_id STRING,
        user_id STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        event_type STRING,
        product_id STRING,
        browser STRING,
        operating_system STRING,
        country STRING,
        city STRING,
       
        event_timestamp TIMESTAMP,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    """

    session.sql(create_events_sql).collect()
    session.sql(create_sessions_sql).collect()

    logging.info("Silver tables ensured.")



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



def clean_events(session: Session) -> None:
    logging.info("Starting clean events incremental load...")
    last_ingested_at = get_last_ingested_at(session, EVENTS_SILVER_TABLE)

    events_df = session.table(EVENTS_TABLE).filter(col("ingested_at").cast("timestamp") > last_ingested_at)
    if events_df.count() == 0:
        logging.info("No new event records to process.")
        return
    events_cleaned = (
        events_df
        .with_column("event_type",lower(col("event_type")))
        .filter(
            (col("event_id").is_not_null()) &
            (col("user_id").is_not_null()) &
            (col("event_type").is_not_null()) &
            (col("timestamp").is_not_null()) 
        )
        .drop_duplicates(["event_id"])
    )
    if events_cleaned.count() == 0:
        logging.info("No valid records after cleaning")
        return
    
    # Write staging table and merge
    logging.info(f"Writing cleaned events to {EVENTS_STAGING_TABLE}...")
    events_cleaned.write.save_as_table(EVENTS_STAGING_TABLE, mode="overwrite")

    #If silver table doesnt exist create it with the new cleaned events
    try:
        target_table = session.table(EVENTS_SILVER_TABLE)
    except Exception:
        logging.info(f"Target table {EVENTS_SILVER_TABLE} not found; creating new.")
        events_cleaned.write.save_as_table(EVENTS_SILVER_TABLE, mode="overwrite")
        return

    #If silver table does exists we continue with the merge
    staging_df = session.table(EVENTS_STAGING_TABLE)
    try:
        merge_result: MergeResult  = target_table.merge(
            staging_df,
            target_table["event_id"] == staging_df["event_id"],
            [
                when_matched().update({
                    "user_id": staging_df["user_id"],
                    "event_type": staging_df["event_type"],
                    "product_id": staging_df["product_id"],
                    "timestamp": staging_df["timestamp"],
                    "ingested_at": staging_df["ingested_at"]
                }),
                when_not_matched().insert({
                    "event_id": staging_df["event_id"],
                    "user_id": staging_df["user_id"],
                    "event_type": staging_df["event_type"],
                    "product_id": staging_df["product_id"],
                    "timestamp": staging_df["timestamp"],
                    "ingested_at": staging_df["ingested_at"]
                })
            ]
        )
        logging.info(f"Events merged: {merge_result.rows_inserted} inserted, {merge_result.rows_updated} updated.")
    except Exception as e:
        logging.error(f"Merge failed for {EVENTS_SILVER_TABLE}: {e}")
        raise

def flatten_session(session: Session) -> None:
    logging.info("Starting flatten_sessions incremental load...")
    last_ingested_at = get_last_ingested_at(session, SESSIONS_SILVER_TABLE)

    session_df = session.table(SESSIONS_TABLE).filter(col("ingested_at").cast("timestamp") > last_ingested_at)

    if session_df.count() == 0:
        logging.info("No new session records to process")
        return

    session_events_flat = (
        session_df
        .join_table_function(
            "flatten",
            input=col("events")
        )
        .select(
            col("session_id"),
            col("user_id"),
            col("start_time"),
            col("end_time"),
            lower(col("value")["type"]).as_("event_type"),
            col("value")["product_id"].as_("product_id"),
            col("device")["browser"].as_("browser"),
            col("device")["os"].as_("operating_system"),
            col("location")["country"].as_("country"),
            col("location")["city"].as_("city"),
            col("value")["timestamp"].cast("timestamp").as_("event_timestamp"),
            col("ingested_at")
        )
        .drop_duplicates(["session_id", "event_type", "event_timestamp"])
    )






    if session_events_flat.count() == 0:
        logging.info("No new flattened session data")
        return
    
    logging.info(f"Writing flattened sessions to {SESSIONS_STAGING_TABLE}...")
    session_events_flat.write.save_as_table(SESSIONS_STAGING_TABLE,mode="overwrite")

    try:
        target_table = session.table(SESSIONS_SILVER_TABLE)
    except Exception:
        logging.info(f"Target table {SESSIONS_SILVER_TABLE} not found; creating new.")
        session_events_flat.write.save_as_table(SESSIONS_SILVER_TABLE, mode="overwrite")
        return
    
    staging_df = session.table(SESSIONS_STAGING_TABLE)
    try:
        merge_result: MergeResult = target_table.merge(
            staging_df,
            (target_table["session_id"] == staging_df["session_id"])&
            (target_table["event_type"] == staging_df["event_type"])&
            (target_table["event_timestamp"] == staging_df["event_timestamp"]),
            [
                when_matched().update({
                    "user_id": staging_df["user_id"],
                    "start_time": staging_df["start_time"],
                    "end_time": staging_df["end_time"],
                    "product_id": staging_df["product_id"],
                    "browser": staging_df["browser"],
                    "operating_system": staging_df["operating_system"],
                    "country": staging_df["country"],
                    "city": staging_df["city"],
                    "ingested_at": staging_df["ingested_at"]
                }),
                when_not_matched().insert({
                    "session_id": staging_df["session_id"],
                    "user_id": staging_df["user_id"],
                    "start_time": staging_df["start_time"],
                    "end_time": staging_df["end_time"],
                    "event_type": staging_df["event_type"],
                    "product_id": staging_df["product_id"],
                    "browser": staging_df["browser"],
                    "operating_system": staging_df["operating_system"],
                    "country": staging_df["country"],
                    "city": staging_df["city"],
                    "event_timestamp": staging_df["event_timestamp"],
                    "ingested_at": staging_df["ingested_at"]
                })
            ]
        )
        logging.info(f"Sessions merged: {merge_result.rows_inserted} inserted, {merge_result.rows_updated} updated.")
    except Exception as e:
        logging.error(f"Merge failed for {SESSIONS_SILVER_TABLE} : {e}")
        raise



def main(step: str, env_path: str):
   
    # Check if file exists
    if not os.path.exists(env_path):
        raise FileNotFoundError(f".env file not found at path: {env_path}")
    #Defualt Configs
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
    ensure_tables(session)
    try:
        if step in ["all","events"]:
            clean_events(session)
        if step in ["all","sessions"]:
            flatten_session(session)
    finally:
        session.close()
        logging.info("Snowpark session closed.")
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", choices=["all", "events", "sessions"], default="all")
    parser.add_argument("--env", default=".env", help="Path to .env file")
    args = parser.parse_args()
    main(args.step, args.env)
