#!/usr/bin/env python3
"""
Peloton Data Pipeline
Handles authentication automatically and runs the user's data processing logic.
"""

import logging
import time
import sys
import os
import json
from pathlib import Path
from datetime import datetime, timezone

import requests
import duckdb
import pandas as pd
from dotenv import load_dotenv

# Import the token exchange module
import peloton_token_exchange as auth

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load .env file (for local MOTHERDUCK_TOKEN, etc.)
load_dotenv()

# Constants
MD_DB = "digma_demo"

# Ensure MOTHERDUCK_TOKEN is set
if "MOTHERDUCK_TOKEN" not in os.environ:
    logger.warning("MOTHERDUCK_TOKEN not found in environment variables. MotherDuck connection may fail.")

def get_valid_auth_headers() -> dict:
    """
    Ensures we have a valid access token and returns the auth headers.
    Checks the local expiration time and refreshes if expired or expiring soon.
    """
    token_path = Path(auth.TOKENS_FILE).resolve()
    
    # 1. Load tokens
    try:
        tokens = auth.load_tokens(token_path)
    except Exception as e:
        logger.error(f"Failed to load tokens: {e}")
        sys.exit(1)
        
    # 2. Check expiration
    # Refresh if expired or expires in less than 1 day
    now = time.time()
    expires_at = tokens.get("expires_at", 0)
    buffer_seconds = 86400 
    
    if now >= (expires_at - buffer_seconds):
        logger.info("Token is expired or expiring soon. Refreshing...")
        
        if "refresh_token" not in tokens:
            logger.error("No refresh token available to refresh authentication.")
            sys.exit(1)
            
        try:
            # Refresh
            refresh_resp = auth.refresh_tokens(tokens["refresh_token"])
            
            # Update data
            tokens = auth.update_token_data(tokens, refresh_resp)
            
            # Save
            auth.save_tokens(token_path, tokens)
            
        except Exception as e:
            logger.error(f"Token refresh failed: {e}")
            sys.exit(1)
    else:
        logger.info("Current access token is valid.")

    # 3. Construct headers
    return {
        "Authorization": f'{tokens.get("token_type", "Bearer")} {tokens["access_token"]}',
        "User-Agent": "peloton-client/1.0"
    }

def get_latest_workout_timestamp(con) -> int:
    """Query MotherDuck for the most recent workout start time."""
    try:
        # Check if table exists first
        # Note: information_schema might behave differently depending on DB, 
        # but try/except block handles failures gracefully.
        try:
            table_exists = con.execute("""
                SELECT count(*) FROM information_schema.tables 
                WHERE table_schema = 'peloton' AND table_name = 'workouts_raw'
            """).fetchone()[0] > 0
        except:
            # Fallback if information_schema query fails, assume table might not exist or query directly
            table_exists = False

        if not table_exists:
            # Double check by trying to select from it? Or just return 0
            # If we simply return 0, we fetch everything.
            logger.info("Table peloton.workouts_raw may not exist or is empty. Defaulting to full history.")
            return 0
            
        result = con.execute("SELECT MAX(start_time) FROM peloton.workouts_raw").fetchone()
        if result and result[0]:
            # Ensure we're working with a timestamp object
            max_time = result[0]
            # DuckDB might return datetime object or string depending on driver/version
            if isinstance(max_time, str):
                max_time = datetime.fromisoformat(max_time)
            
            logger.info(f"Most recent workout in DB: {max_time}")
            return int(max_time.timestamp())
        return 0
    except Exception as e:
        logger.warning(f"Could not fetch max date from MotherDuck ({e}). Defaulting to full history.")
        return 0

def get_workout_details(session: requests.Session, workout_id: str) -> dict:
    """Fetch detailed metrics for a workout."""
    resp = session.get(f'{auth.API_BASE}/api/workout/{workout_id}')
    # Some workouts might fail, allow calling code to handle or suppress
    resp.raise_for_status()
    details = resp.json()
    if 'ride' in details:
        details['ride_id'] = details['ride']['id']
    return details

def get_ride_details(session: requests.Session, ride_id: str) -> dict:
    """Fetch metadata for the ride associated with a workout."""
    resp = session.get(f'{auth.API_BASE}/api/ride/{ride_id}/details')
    resp.raise_for_status()
    return resp.json()

def get_workout_history(session: requests.Session, user_id: str, cutoff_timestamp: int) -> list:
    """
    Fetch all workouts since the cutoff_timestamp.
    Iterates through pages until it hits a workout older than the cutoff.
    """
    page = 0
    keep_going = True
    workouts = []
    start_time = time.time()
    
    logger.info(f"Fetching workouts for user {user_id} since epoch {cutoff_timestamp}...")

    while keep_going:
        logger.info(f"Pulling page {page}...")
        resp = session.get(f'{auth.API_BASE}/api/user/{user_id}/workouts?limit=100&page={page}')
        
        if resp.status_code != 200:
            logger.error(f"Error fetching page {page}: {resp.status_code}")
            break
            
        try:
            content = resp.json()
        except:
            logger.error("Failed to parse JSON response")
            break

        data = content.get('data', [])
        
        if not data:
            break

        for i, wo in enumerate(data):
            # Check date cutoff immediately
            # API returns start_time as unix epoch
            if wo['start_time'] <= cutoff_timestamp:
                logger.info(f"Reached cutoff date (workout time: {wo['start_time']}, cutoff: {cutoff_timestamp}). Stopping.")
                keep_going = False
                break

            # Fetch enrichment data
            try:
                wo['workout_details'] = get_workout_details(session, wo['id'])
                ride_id = wo.get('workout_details', {}).get('ride_id')
                
                if ride_id:
                    wo['ride_details'] = get_ride_details(session, ride_id)
            except Exception as e:
                logger.warning(f"Failed to fetch details for workout {wo['id']}: {e}")

            workouts.append(wo)

            if len(workouts) % 10 == 0:
                elapsed = time.time() - start_time
                logger.info(f"Processed {len(workouts)} workouts in {elapsed:.2f}s")

        # Check pagination flags
        if keep_going:
            if not content.get('show_next'):
                keep_going = False
            else:
                page += 1
                
    return workouts

def to_raw_df(workouts: list) -> pd.DataFrame:
    """Convert list of workout dicts to a DataFrame suitable for MotherDuck."""
    now = datetime.now(timezone.utc)
    return pd.DataFrame([{
        "workout_id": w["id"],
        "start_time": w["start_time"],
        "payload": json.dumps(w, separators=(",", ":")),
        "fetched_at": now,
    } for w in workouts])

def upsert_workouts_raw(con, df: pd.DataFrame):
    """Upsert data into MotherDuck table."""
    con.register("raw_df", df)
    
    # Ensure schema exists (optional, good practice)
    con.execute("CREATE SCHEMA IF NOT EXISTS peloton;")
    
    # Create table if it doesn't exist
    con.execute("""
        CREATE TABLE IF NOT EXISTS peloton.workouts_raw (
            workout_id VARCHAR,
            start_time TIMESTAMP,
            payload JSON,
            fetched_at TIMESTAMP,
            PRIMARY KEY (workout_id)
        );
    """)

    logger.info("Performing upsert...")
    con.execute("CREATE TEMP TABLE stage_raw AS SELECT * FROM raw_df;")

    # Delete existing records that match IDs in new batch
    con.execute("""
        DELETE FROM peloton.workouts_raw
        USING stage_raw
        WHERE peloton.workouts_raw.workout_id = stage_raw.workout_id;
    """)

    # Insert new records
    con.execute("""
        INSERT INTO peloton.workouts_raw
        SELECT * FROM stage_raw;
    """)

    con.execute("DROP TABLE stage_raw;")
    logger.info("Upsert complete.")

def run_pipeline(headers: dict):
    """Main pipeline execution logic."""
    
    # 1. Setup Session
    session = requests.Session()
    session.headers.update(headers)
    
    # 2. Get User ID
    try:
        resp = session.get(f'{auth.API_BASE}/api/me')
        resp.raise_for_status()
        user_id = resp.json()['id']
        logger.info(f"Authenticated as User ID: {user_id}")
    except Exception as e:
        logger.error(f"Failed to get user info: {e}")
        return

    # 3. Connect to MotherDuck & Get Cutoff Date
    con = None
    try:
        logger.info(f"Connecting to MotherDuck ({MD_DB})...")
        con = duckdb.connect(f"md:{MD_DB}")
        
        cutoff_timestamp = get_latest_workout_timestamp(con)
        logger.info(f"Fetching data newer than timestamp: {cutoff_timestamp}")
        
    except Exception as e:
        logger.error(f"MotherDuck connection failed: {e}")
        if con:
            con.close()
        return

    # 4. Fetch Data
    try:
        workouts = get_workout_history(session, user_id, cutoff_timestamp)
        
        if not workouts:
            logger.info("No new workouts found.")
            return

        # 5. Transform
        logger.info(f"Transforming {len(workouts)} workouts...")
        df = to_raw_df(workouts)
        # Convert start_time to proper datetime for DB
        df['start_time'] = pd.to_datetime(df['start_time'], unit='s')
        
        # 6. Load
        upsert_workouts_raw(con, df)
        logger.info(f"Successfully loaded {len(df)} records.")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
    finally:
        if con:
            con.close()

def main():
    logger.info("Initializing Peloton Pipeline...")
    
    # Get valid headers (refreshing if necessary)
    headers = get_valid_auth_headers()
    
    # Run the pipeline
    run_pipeline(headers)
    
    logger.info("Pipeline completed.")

if __name__ == "__main__":
    main()
