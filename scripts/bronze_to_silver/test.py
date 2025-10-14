import os
from dotenv import load_dotenv

def main(env_path: str):
    # Load .env file
    if not os.path.exists(env_path):
        raise FileNotFoundError(f".env file not found at path: {env_path}")
    load_dotenv(dotenv_path=env_path)

    connection_params = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }

    missing = [k for k,v in connection_params.items() if not v]
    if missing:
        print(f"Missing env variables: {', '.join(missing)}")
    else:
        print("All required env variables are present.")
    
    print("Connection params:")
    for k,v in connection_params.items():
        print(f"  {k}: {v}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", default=".env", help="Path to .env file")
    args = parser.parse_args()

    main(args.env)
