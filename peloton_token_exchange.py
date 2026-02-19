#!/usr/bin/env python3
"""
Peloton Token Exchange Script
Exchanges a refresh token for a new access token and updates the local tokens file.
"""

import json
import time
import sys
import logging
from pathlib import Path
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Constants
AUTH0_BASE = "https://auth.onepeloton.com"
TOKEN_ENDPOINT = f"{AUTH0_BASE}/oauth/token"
API_BASE = "https://api.onepeloton.com"
CLIENT_ID = "WVoJxVDdPoFx4RNewvvg6ch2mZ7bwnsM"
TOKENS_FILE = "peloton_tokens.json"

def load_tokens(file_path: Path) -> dict:
    """Load tokens from the JSON file."""
    if not file_path.exists():
        logger.error(f"Tokens file not found at: {file_path}")
        raise FileNotFoundError(f"Tokens file not found at: {file_path}")
        
    try:
        with open(file_path, "r") as f:
            tokens = json.load(f)
            logger.info("Loaded existing tokens.")
            return tokens
    except json.JSONDecodeError:
        logger.error(f"Failed to parse JSON from {file_path}")
        raise ValueError(f"Failed to parse JSON from {file_path}")
    except Exception as e:
        logger.error(f"Error loading tokens: {e}")
        raise

def save_tokens(file_path: Path, tokens: dict) -> None:
    """Save tokens to the JSON file."""
    try:
        with open(file_path, "w") as f:
            json.dump(tokens, f, indent=2)
        logger.info(f"Successfully updated tokens in {file_path}")
    except Exception as e:
        logger.error(f"Error saving tokens: {e}")
        raise

def refresh_tokens(refresh_token: str) -> dict:
    """Perform the token refresh exchange."""
    payload = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "refresh_token": refresh_token,
    }
    
    try:
        logger.info("Attempting to refresh token...")
        resp = requests.post(TOKEN_ENDPOINT, json=payload, timeout=30)
        
        if resp.status_code >= 400:
            logger.error(f"Refresh failed with status {resp.status_code}")
            try:
                logger.error(f"Details: {resp.text[:500]}")
            except:
                pass
            resp.raise_for_status()
            
        return resp.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error during token refresh: {e}")
        raise

def update_token_data(old_tokens: dict, refresh_response: dict) -> dict:
    """Update the token dictionary with new values."""
    now = int(time.time())
    new_tokens = dict(old_tokens)

    new_tokens["access_token"] = refresh_response["access_token"]
    new_tokens["token_type"] = refresh_response.get("token_type", old_tokens.get("token_type", "Bearer"))
    new_tokens["scope"] = refresh_response.get("scope", old_tokens.get("scope"))
    
    expires_in = int(refresh_response.get("expires_in", 0))
    new_tokens["expires_at"] = now + expires_in if expires_in else now
    
    if "refresh_token" in refresh_response and refresh_response["refresh_token"]:
        new_tokens["refresh_token"] = refresh_response["refresh_token"]
        logger.info("Received new refresh token (token rotation active).")
    else:
        logger.info("No new refresh token received; keeping existing one.")

    return new_tokens

def validate_token(tokens: dict) -> None:
    """Validate the new token by calling the /api/me endpoint."""
    headers = {
        "Authorization": f'{tokens.get("token_type", "Bearer")} {tokens["access_token"]}'
    }
    
    try:
        logger.info("Validating new token...")
        resp = requests.get(f"{API_BASE}/api/me", headers=headers, timeout=30)
        
        if resp.status_code != 200:
            logger.error(f"Validation failed with status {resp.status_code}")
            logger.error(f"Response: {resp.text[:300]}")
            resp.raise_for_status()
            
        user_data = resp.json()
        user_id = user_data.get('id', 'Unknown')
        username = user_data.get('username', 'Unknown')
        
        print(f"\\n{'='*50}")
        print("✅ SUCCESS: Token refreshed and validated!")
        print(f"User ID: {user_id}")
        print(f"Username: {username}")
        print(f"{'='*50}\\n")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Validation request failed: {e}")
        raise

def main():
    token_path = Path(TOKENS_FILE).resolve()
    
    # 1. Load existing tokens
    current_tokens = load_tokens(token_path)
    
    if "refresh_token" not in current_tokens:
        logger.error("No 'refresh_token' found in the tokens file.")
        sys.exit(1)
        
    try:
        # 2. Refresh the token
        refresh_resp = refresh_tokens(current_tokens["refresh_token"])
        
        # 3. Update token data structure
        updated_tokens = update_token_data(current_tokens, refresh_resp)
        
        # 4. Save updated tokens
        save_tokens(token_path, updated_tokens)
        
        # 5. Validate
        validate_token(updated_tokens)
        
    except Exception as e:
        logger.error(f"Process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()