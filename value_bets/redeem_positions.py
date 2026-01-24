#!/usr/bin/env python3
"""
Automatically redeem winning positions on Polymarket from trades.csv.

Reads trades.csv every 10 minutes and redeems positions for unique condition_ids
using the CTF contract via gasless relayer execution.

Requirements:
    - pandas
    - web3
    - py-builder-relayer-client
    - python-dotenv (optional, for .env file support)

Setup:
    1. Install dependencies:
       pip install pandas web3 py-builder-relayer-client py-builder-signing-sdk python-dotenv
    
    2. Get Builder API credentials from Polymarket Builder Profile:
       https://polymarket.com/settings?tab=builder
    
    3. Set environment variables (or create a .env file):
       export POLYMARKET_KEY=your_private_key_here
       export POLY_BUILDER_API_KEY=your_builder_api_key
       export POLY_BUILDER_SECRET=your_builder_secret
       export POLY_BUILDER_PASSPHRASE=your_builder_passphrase
       
       Or create a .env file in the parent directory with these variables.
    
    4. Ensure trades.csv has a 'condition_id' column
    
    5. Run:
       python3 redeem_positions.py

The script will:
    - Read trades.csv every 10 minutes
    - Extract unique condition_ids
    - Redeem positions via CTF contract using gasless relayer
    - Log all activities to redeem_positions.log and stdout
    - Handle already-redeemed positions gracefully
"""

import os
import sys
import time
import logging
import traceback
from typing import Set, Optional
from datetime import datetime

import pandas as pd
from web3 import Web3
from eth_abi import encode
from eth_utils import keccak, to_bytes, to_checksum_address

# Load environment variables
try:
    from dotenv import load_dotenv
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    load_dotenv(env_path)
    load_dotenv()
except ImportError:
    pass

try:
    from py_builder_relayer_client.client import RelayClient, SafeTransaction
    from py_builder_relayer_client.models import OperationType
except ImportError:
    print("Error: py-builder-relayer-client not installed. Install with: pip install py-builder-relayer-client")
    sys.exit(1)

try:
    from py_builder_signing_sdk.config import BuilderConfig, BuilderApiKeyCreds
except ImportError:
    print("Error: py-builder-signing-sdk not installed. Install with: pip install py-builder-signing-sdk")
    sys.exit(1)


# Constants
CTF_CONTRACT = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
USDCe_TOKEN = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
POLYGON_CHAIN_ID = 137
REDEEM_INTERVAL_SECONDS = 600  # 10 minutes
RELAYER_URL = "https://relayer-v2.polymarket.com/"  # Production relayer URL

# CSV file path (relative to script location)
TRADES_CSV_PATH = os.path.join(os.path.dirname(__file__), "trades.csv")


def setup_logging() -> None:
    """Setup logging configuration."""
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler("redeem_positions.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def int_to_bytes32(value: int) -> bytes:
    """Convert integer to bytes32 (32 bytes, big-endian)."""
    return value.to_bytes(32, byteorder="big")


def bytes32_to_int(value: bytes) -> int:
    """Convert bytes32 to integer."""
    return int.from_bytes(value, byteorder="big")


def load_trades_csv(csv_path: str) -> pd.DataFrame:
    """
    Load trades from CSV file.
    
    Returns:
        DataFrame with trades, or empty DataFrame if file doesn't exist or is empty.
    """
    if not os.path.exists(csv_path):
        logging.warning(f"CSV file not found: {csv_path}")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            logging.info("CSV file is empty")
            return df
        
        # Check if condition_id column exists
        if "condition_id" not in df.columns:
            logging.warning("condition_id column not found in CSV. Make sure trades.csv includes condition_id column.")
            return pd.DataFrame()
        
        # Filter out rows with empty condition_id
        initial_count = len(df)
        df = df[df["condition_id"].notna()]
        df = df[df["condition_id"].astype(str) != ""]
        df = df[df["condition_id"].astype(str).str.strip() != ""]
        
        filtered_count = len(df)
        if initial_count != filtered_count:
            logging.info(f"Filtered out {initial_count - filtered_count} rows with empty condition_id")
        
        logging.info(f"Loaded {filtered_count} trades with valid condition_ids from {csv_path}")
        return df
    except Exception as e:
        logging.error(f"Error reading CSV file {csv_path}: {e}")
        traceback.print_exc()
        return pd.DataFrame()


def get_unique_condition_ids(df: pd.DataFrame) -> Set[str]:
    """
    Extract unique condition_ids from trades DataFrame.
    
    Returns:
        Set of condition_id strings (as they appear in CSV).
    """
    if df.empty or "condition_id" not in df.columns:
        return set()
    
    condition_ids = df["condition_id"].dropna().astype(str)
    condition_ids = condition_ids[condition_ids != ""]
    unique_ids = set(condition_ids.unique())
    logging.info(f"Found {len(unique_ids)} unique condition_ids")
    return unique_ids


def create_redeem_transaction(condition_id_str: str) -> Optional[SafeTransaction]:
    """
    Create a redeemPositions transaction for a given condition_id.
    
    Args:
        condition_id_str: Condition ID as string (from CSV)
        
    Returns:
        SafeTransaction object ready for relayer, or None if error.
    """
    try:
        # Convert condition_id string to int, then to bytes32
        # Handle different formats: decimal string, hex string (with or without 0x)
        condition_id_str_clean = condition_id_str.strip()
        
        if condition_id_str_clean.startswith("0x"):
            # Hex format
            condition_id_int = int(condition_id_str_clean, 16)
        else:
            # Decimal format
            condition_id_int = int(condition_id_str_clean)
        
        condition_id_bytes = int_to_bytes32(condition_id_int)
        
        # Convert addresses to checksum format
        ctf_contract_checksum = to_checksum_address(CTF_CONTRACT)
        usdce_token_checksum = to_checksum_address(USDCe_TOKEN)
        
        # Manually encode the function call (no provider needed)
        # Function signature: redeemPositions(address,bytes32,bytes32,uint256[])
        function_signature = "redeemPositions(address,bytes32,bytes32,uint256[])"
        
        # Get function selector (first 4 bytes of keccak256 hash of signature)
        selector = keccak(to_bytes(text=function_signature))[:4]
        
        # Encode parameters
        encoded_params = encode(
            ['address', 'bytes32', 'bytes32', 'uint256[]'],
            [
                usdce_token_checksum,  # collateralToken
                bytes(32),            # parentCollectionId (null/empty)
                condition_id_bytes,   # conditionId
                [1, 2]                # indexSets (both YES=1 and NO=2)
            ]
        )
        
        # Combine selector + encoded parameters
        data = "0x" + (selector + encoded_params).hex()
        
        # Create SafeTransaction object
        return SafeTransaction(
            to=ctf_contract_checksum,
            operation=OperationType.Call,
            data=data,
            value="0"
        )
    except ValueError as e:
        logging.error(f"Invalid condition_id format '{condition_id_str}': {e}")
        return None
    except Exception as e:
        logging.error(f"Error creating redeem transaction for condition_id {condition_id_str}: {e}")
        traceback.print_exc()
        return None


def redeem_condition_ids(
    condition_ids: Set[str],
    relay_client: RelayClient,
    max_batch_size: int = 10
) -> dict:
    """
    Redeem positions for multiple condition_ids.
    
    Args:
        condition_ids: Set of condition_id strings to redeem
        relay_client: Initialized RelayClient
        max_batch_size: Maximum transactions per batch
        
    Returns:
        Dict with success/failure counts
    """
    if not condition_ids:
        logging.info("No condition_ids to redeem")
        return {"success": 0, "failed": 0, "skipped": 0}
    
    results = {"success": 0, "failed": 0, "skipped": 0}
    
    # Process in batches to avoid overwhelming the relayer
    condition_id_list = list(condition_ids)
    for i in range(0, len(condition_id_list), max_batch_size):
        batch = condition_id_list[i:i + max_batch_size]
        logging.info(f"Processing batch {i // max_batch_size + 1} ({len(batch)} condition_ids)")
        
        transactions = []
        batch_condition_ids = []
        
        for condition_id in batch:
            tx = create_redeem_transaction(condition_id)
            if tx is None:
                results["skipped"] += 1
                continue
            transactions.append(tx)
            batch_condition_ids.append(condition_id)
        
        if not transactions:
            logging.warning("No valid transactions in batch, skipping")
            continue
        
        # Execute batch via relayer
        try:
            logging.info(f"Executing {len(transactions)} redeem transactions via relayer...")
            logging.info(f"Condition IDs in batch: {batch_condition_ids}")
            
            response = relay_client.execute(transactions, "Redeem positions")
            
            # Wait for execution
            logging.info("Waiting for relayer execution...")
            response.wait()
            
            # If wait() completes without exception, assume success
            # The relayer will handle errors and exceptions will be caught below
            logging.info(f"Successfully redeemed {len(transactions)} condition_ids")
            results["success"] += len(transactions)
                
        except Exception as e:
            error_str = str(e).lower()
            # Check if error indicates positions already redeemed
            if ("already redeemed" in error_str or 
                "no positions" in error_str or
                "nothing to redeem" in error_str or
                "revert" in error_str and "redeem" in error_str):
                logging.info(f"Positions may already be redeemed (error: {e})")
                results["success"] += len(transactions)  # Count as handled
            else:
                logging.error(f"Error executing redeem batch: {e}")
                traceback.print_exc()
                results["failed"] += len(transactions)
        
        # Small delay between batches
        if i + max_batch_size < len(condition_id_list):
            time.sleep(2)
    
    return results


def main_loop(relay_client: RelayClient) -> None:
    """
    Main loop that runs every 10 minutes to redeem positions.
    
    Args:
        relay_client: Initialized RelayClient
    """
    logging.info("Starting redemption loop (every 10 minutes)")
    logging.info(f"Reading trades from: {TRADES_CSV_PATH}")
    
    iteration = 0
    
    while True:
        iteration += 1
        logging.info(f"\n{'='*80}")
        logging.info(f"REDEMPTION ITERATION #{iteration}")
        logging.info(f"Time: {datetime.now().isoformat()}")
        logging.info(f"{'='*80}")
        
        try:
            # Load trades from CSV
            df = load_trades_csv(TRADES_CSV_PATH)
            
            if df.empty:
                logging.info("No trades found, skipping redemption")
            else:
                # Get unique condition_ids
                condition_ids = get_unique_condition_ids(df)
                
                if condition_ids:
                    # Redeem positions
                    results = redeem_condition_ids(condition_ids, relay_client)
                    logging.info(f"\nRedemption results:")
                    logging.info(f"  Success: {results['success']}")
                    logging.info(f"  Failed: {results['failed']}")
                    logging.info(f"  Skipped: {results['skipped']}")
                else:
                    logging.info("No valid condition_ids found to redeem")
        
        except KeyboardInterrupt:
            logging.info("\nReceived interrupt signal, shutting down...")
            break
        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}")
            traceback.print_exc()
        
        # Wait 10 minutes before next iteration
        logging.info(f"\nWaiting {REDEEM_INTERVAL_SECONDS / 60:.1f} minutes until next redemption cycle...")
        time.sleep(REDEEM_INTERVAL_SECONDS)


def validate_environment_variables() -> tuple[str, BuilderConfig]:
    """
    Validate that all required environment variables are set.
    
    Returns:
        Tuple of (private_key, builder_config)
        
    Raises:
        SystemExit if any required variables are missing
    """
    # Required environment variables
    required_vars = {
        "POLYMARKET_KEY": "Private key for signing transactions",
        "POLY_BUILDER_API_KEY": "Builder API key from Polymarket Builder Profile",
        "POLY_BUILDER_SECRET": "Builder API secret from Polymarket Builder Profile",
        "POLY_BUILDER_PASSPHRASE": "Builder API passphrase from Polymarket Builder Profile",
    }
    
    missing_vars = []
    env_values = {}
    
    for var_name, description in required_vars.items():
        value = os.getenv(var_name)
        if not value:
            missing_vars.append((var_name, description))
        else:
            env_values[var_name] = value
    
    if missing_vars:
        logging.error("Missing required environment variables:")
        for var_name, description in missing_vars:
            logging.error(f"  - {var_name}: {description}")
        logging.error("\nPlease set these variables or add them to your .env file.")
        logging.error("Get Builder API credentials from: https://polymarket.com/settings?tab=builder")
        logging.error("\nExample .env file:")
        logging.error("  POLYMARKET_KEY=your_private_key_here")
        logging.error("  POLY_BUILDER_API_KEY=your_builder_api_key")
        logging.error("  POLY_BUILDER_SECRET=your_builder_secret")
        logging.error("  POLY_BUILDER_PASSPHRASE=your_builder_passphrase")
        return None, None
    
    # Create Builder API credentials
    builder_creds = BuilderApiKeyCreds(
        key=env_values["POLY_BUILDER_API_KEY"],
        secret=env_values["POLY_BUILDER_SECRET"],
        passphrase=env_values["POLY_BUILDER_PASSPHRASE"]
    )
    
    # Create Builder config
    builder_config = BuilderConfig(
        local_builder_creds=builder_creds
    )
    
    return env_values["POLYMARKET_KEY"], builder_config


def check_and_deploy_safe_wallet(relay_client: RelayClient) -> bool:
    """
    Check if Safe wallet is deployed, and deploy it if not.
    
    Args:
        relay_client: Initialized RelayClient
        
    Returns:
        True if Safe wallet is deployed (or deployment succeeded), False otherwise
    """
    try:
        # Check if Safe wallet is already deployed
        logging.info("Checking if Safe wallet is deployed...")
        deployed_info = relay_client.get_deployed()
        
        if deployed_info and hasattr(deployed_info, 'proxy_address') and deployed_info.proxy_address:
            safe_address = deployed_info.proxy_address
            logging.info(f"Safe wallet already deployed at: {safe_address}")
            return True
        
        # Safe wallet not deployed, deploy it now
        logging.info("Safe wallet not deployed. Deploying now...")
        logging.info("This may take a few moments...")
        
        deploy_response = relay_client.deploy()
        deploy_result = deploy_response.wait()
        
        if deploy_result and hasattr(deploy_result, 'proxy_address') and deploy_result.proxy_address:
            safe_address = deploy_result.proxy_address
            logging.info(f"✓ Safe wallet deployed successfully at: {safe_address}")
            return True
        else:
            logging.error("Failed to deploy Safe wallet: No proxy address returned")
            return False
            
    except Exception as e:
        error_str = str(e).lower()
        # Check if error indicates wallet is already deployed
        if "already deployed" in error_str or "exists" in error_str:
            logging.info("Safe wallet appears to be already deployed (based on error message)")
            return True
        else:
            logging.error(f"Error checking/deploying Safe wallet: {e}")
            traceback.print_exc()
            return False


def main() -> int:
    """Main entry point."""
    setup_logging()
    
    # Validate environment variables and get credentials
    private_key, builder_config = validate_environment_variables()
    if private_key is None or builder_config is None:
        return 1
    
    # Initialize relayer client with Builder API credentials
    try:
        logging.info("Initializing RelayClient with Builder API credentials...")
        relay_client = RelayClient(
            relayer_url=RELAYER_URL.rstrip("/"),  # Remove trailing slash if present
            chain_id=POLYGON_CHAIN_ID,
            private_key=private_key,
            builder_config=builder_config
        )
        logging.info("RelayClient initialized successfully")
    except Exception as e:
        logging.error(f"Failed to initialize RelayClient: {e}")
        logging.error("Please verify your Builder API credentials are correct.")
        logging.error("Get credentials from: https://polymarket.com/settings?tab=builder")
        traceback.print_exc()
        return 1
    
    # Check and deploy Safe wallet if needed
    if not check_and_deploy_safe_wallet(relay_client):
        logging.error("Failed to deploy Safe wallet. Cannot proceed with redemptions.")
        logging.error("Please check the error messages above and try again.")
        return 1
    
    logging.info("Safe wallet is ready. Proceeding with redemption loop...")
    
    # Run main loop
    try:
        main_loop(relay_client)
    except KeyboardInterrupt:
        logging.info("\nShutting down gracefully...")
        return 0
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
        raise SystemExit(exit_code)
    except KeyboardInterrupt:
        print("\n[INTERRUPTED] Script interrupted by user. Exiting gracefully...")
        raise SystemExit(0)
    except Exception as e:
        print(f"\n{'!'*60}")
        print(f"!!! UNEXPECTED ERROR !!!")
        print(f"{'!'*60}")
        print(f"  Error: {e}")
        traceback.print_exc()
        raise SystemExit(1)
