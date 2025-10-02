import csv
import json
import os 
import random
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

#Default Configs
DEFAULT_NUM_USERS = 10
DEFAULT_NUM_EVENTS = 50
DEFAULT_NUM_SESSIONS = 10

PRODUCT_IDS: List[str] = [f"PROD_{i:03}" for i in range(1,11)]
EVENT_TYPES: List[str] = ["view_product","add_to_cart","remove_from_cart","purchase"]
CITIES: List[str] = ["Oslo","Bergen","Stavanger","Trondheim"]
BROWSERS: List[str] = ["Chrome","Firefox","Safari","Edge"]
OS_TYPES: List[str] = ["Windows","macOS","Linux","iOS","Android"]

#Folder Stucture
BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR/ "data"/ "raw"/ "events.csv"
JSON_PATH = BASE_DIR/ "data"/ "raw"/ "sessions.json"

CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
JSON_PATH.parent.mkdir(parents=True, exist_ok=True)


#Logging
logging.basicConfig(level=logging.INFO, format = "%(asctime)s [%(levelname)s] %(message)s" 
                    , datefmt="%Y-%m-%d %H:%M:%S",)


# CSV Event Generator
def generate_csv_events(num_users: int, num_events: int, file_path: Path) -> None:
    logging.info(f"Generating CSV events at {file_path}")
    try:
        with open(file_path,mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(["event_id","user_id","event_type","product_id","timestamp"])

            for event_id in range(1,num_events + 1):
                user_id = f"user_{random.randint(1,num_users)}"
                event_type = random.choice(EVENT_TYPES)
                product_id = random.choice(PRODUCT_IDS)
                timestamp = datetime.now() - timedelta(minutes=random.randint(0,5000))
                writer.writerow([event_id, user_id, event_type, product_id,timestamp.isoformat()])
        logging.info("CSV event generation completed successfully") 
    except Exception as e:
        logging.error(f"Failed to generate CSV events: {e}")

# JSON Session Generator

def generate_json_sessions(num_users: int, num_sessions: int, file_path: Path) -> None:
    logging.info(f"Generating JSON sessions at {file_path}")
    sessions: List[Dict[str,Any]] = []

    try:
        for session_id in range(1, num_sessions +1):
            user_id = f"user_{random.randint(1, num_users)}"
            start_time = datetime.now() - timedelta(hours=random.randint(5,100)) 
            num_events = random.randint(2,6)
            events = []
            for i in range(num_events):
                event_time = start_time + timedelta(minutes=random.randint(1,60))
                events.append(
                    {
                        "type":random.choice(EVENT_TYPES),
                        "product_id": random.choice(PRODUCT_IDS),
                        "timestamp": event_time.isoformat(),
                    }
                )
            #Ensure events are ordered by timestamps
            events = sorted(events, key= lambda x: x["timestamp"])
            end_time = events[-1]["timestamp"] if events else start_time.isoformat()

            session = {
                "session_id": f"sess_{session_id}",
                "user_id": user_id,
                "start_time": start_time.isoformat(),
                "end_time": end_time,
                "events": events,
                "device":{
                    "os":random.choice(OS_TYPES),
                    "browser":random.choice(BROWSERS),
                },
                "location":{
                    "country": "Norway",
                    "city": random.choice(CITIES),
                },
            }
            sessions.append(session)
        with open(file_path,mode="w", encoding="utf-8") as f:
            json.dump(sessions,f,indent=2)
        logging.info("JSON session generation completed successfully")
        
    except Exception as e:
        logging.error(f"Failed to generate JSON sessions: {e}")

#CLI
def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic data generator for product events and sessions.")
    parser.add_argument("--users", type=int, default=DEFAULT_NUM_USERS, help="Number of users")
    parser.add_argument("--events", type=int, default=DEFAULT_NUM_EVENTS, help="Number of events to generate")
    parser.add_argument("--sessions", type=int, default=DEFAULT_NUM_SESSIONS, help="Number of sessions to generate")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    parser.add_argument("--csv", type=Path, default=CSV_PATH, help="Output path for CSV events")
    parser.add_argument("--json", type=Path, default=JSON_PATH, help="Output path for JSON sessions")

    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        logging.info(f"Using random seed: {args.seed}")

    logging.info("Starting synthetic data generation...")
    generate_csv_events(args.users, args.events, args.csv)
    generate_json_sessions(args.users, args.sessions, args.json)
    logging.info("Done generating synthetic data.")


if __name__ == "__main__":
    main()