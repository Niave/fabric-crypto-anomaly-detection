import os
import logging
import snowflake.connector
from pathlib import Path
from dotenv import load_dotenv

#Defualt Configs
load_dotenv() #Load credentials found in .env file
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

#Folder Stucture
BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR/ "data"/ "raw"/ "events.csv"
JSON_PATH = BASE_DIR/ "data"/ "raw"/ "sessions.json"

STAGE_NAME = os.getenv("SNOWFLAKE_STAGE","MY_STAGE")

def connect_to_snowflake() -> snowflake.connector.SnowflakeConnection:
    logging.info("Connecting to Snowflake....")
    conn = snowflake.connector.connect(
        user = os.getenv("SNOWFLAKE_USER"),
        password = os.getenv("SNOWFLAKE_PASSWORD"),
        account = os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
        database = os.getenv("SNOWFLAKE_DATABASE"),
        schema = os.getenv("SNOWFLAKE_SCHEMA"),
        role = os.getenv("SNOWFLAKE_ROLE"),
    )
    logging.info("Connected to Snowflake")
    return conn



#Create Tables, Stage and Formats

def setup_schema(conn: snowflake.connector.SnowflakeConnection) -> None:
    with conn.cursor() as cur:
        #Stage
        cur.execute(f"CREATE STAGE IF NOT EXISTS {STAGE_NAME};")

        #File Formats
        cur.execute("""
            CREATE FILE FORMAT IF NOT EXISTS CSV_FORMAT
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY= '"'
            SKIP_HEADER = 1;
        """)

        cur.execute("""
            CREATE FILE FORMAT IF NOT EXISTS JSON_FORMAT
            TYPE = 'JSON';
        """)

        #Tables
        cur.execute("""
            CREATE TABLE IF NOT EXISTS EVENTS(
                event_id INT,
                user_id STRING,
                event_type STRING,
                product_id STRING,
                timestamp TIMESTAMP,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS SESSIONS(
                session_id STRING,
                user_id STRING,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                device VARIANT,
                location VARIANT,
                events VARIANT,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
            );
        """)
    logging.info("Tables, stage and file formats created or verified")

    
def load_csv_events(conn: snowflake.connector.SnowflakeConnection, csv_path: Path) -> None:
    logging.info(f"Loading events from {csv_path} ...")
    with conn.cursor() as cur:
        #Moving file to stage
        cur.execute(f"PUT file://{csv_path} @{STAGE_NAME} OVERWRITE=TRUE")

        merge_sql=f"""
            MERGE INTO EVENTS AS target
            USING @{STAGE_NAME}/{csv_path.name}(FILE_FORMAT => 'CSV_FORMAT') AS source
            ON target.event_id = source.$1
            WHEN MATCHED THEN
                UPDATE SET
                    user_id = source.$2,
                    event_type = source.$3,
                    product_id = source.$4,
                    timestamp = TO_TIMESTAMP(source.$5),
                    ingested_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (event_id,user_id,event_type,product_id,timestamp, ingested_at)
                VALUES (source.$1,source.$2,source.$3,source.$4,TO_TIMESTAMP(source.$5), CURRENT_TIMESTAMP());
        """
        #Merging data from stage to table
        cur.execute(merge_sql)
        #Remove file from stage to prevent unnecessary storage retention
        cur.execute(f"REMOVE @{STAGE_NAME}/{csv_path.name}")
    logging.info("CSV events merged successfully")

def load_json_sessions(conn: snowflake.connector.SnowflakeConnection, json_path: Path) -> None:
    logging.info(f"Loading sessions from {json_path}")
    with conn.cursor() as cur:
        # Stage the file
        cur.execute(f"PUT file://{json_path} @{STAGE_NAME} OVERWRITE=TRUE")

        merge_sql = f"""
            MERGE INTO SESSIONS AS target
            USING(
                SELECT
                    f.value:session_id::STRING AS session_id,
                    f.value:user_id::STRING AS user_id,
                    TO_TIMESTAMP(f.value:start_time::STRING) AS start_time,
                    TO_TIMESTAMP(f.value:end_time::STRING) AS end_time,
                    f.value:device AS device,
                    f.value:location AS location,
                    f.value:events::VARIANT AS events
                FROM @{STAGE_NAME}/{json_path.name} (FILE_FORMAT => 'JSON_FORMAT'),
                LATERAL FLATTEN(input => $1) f
            ) AS source
            ON target.session_id = source.session_id
            WHEN MATCHED THEN
                UPDATE SET
                    user_id = source.user_id,
                    start_time = source.start_time,
                    end_time = source.end_time,
                    device = source.device,
                    location = source.location,
                    events = source.events,
                    ingested_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (session_id, user_id, start_time, end_time, device, location, events, ingested_at)
                VALUES (source.session_id, source.user_id, source.start_time, source.end_time,
                source.device, source.location, source.events, CURRENT_TIMESTAMP());
        """
        cur.execute(merge_sql)

        # Clean up staged file
        cur.execute(f"REMOVE @{STAGE_NAME}/{json_path.name}")

    logging.info("JSON sessions merged successfully")


def main():
    conn = connect_to_snowflake()
    try:
        setup_schema(conn)
        load_csv_events(conn, CSV_PATH)
        load_json_sessions(conn, JSON_PATH)
    finally:
        conn.close()
        logging.info("Snowflake connection closed")


if __name__ == "__main__":
    main()
