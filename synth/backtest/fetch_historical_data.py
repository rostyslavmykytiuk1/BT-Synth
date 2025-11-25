"""
Fetch Historical Miner Data from Synth API

This script fetches historical miner CRPS scores from the Synth API and stores them
in a CSV file. It handles incremental updates by checking existing data and only
fetching new data.
"""

import os
import sys
import pandas as pd
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any
import argparse
import time

# Add parent directories to path for imports
current_dir = os.path.dirname(__file__)
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
synth_subnet_root = os.path.abspath(os.path.join(current_dir, "..", "..", "..", ".."))
sys.path.insert(0, synth_subnet_root)

# API Configuration
API_BASE_URL = "https://api.synthdata.co/validation/scores/historical"
MAX_DAYS_PER_REQUEST = 6  # API limit: 6 days per request


def fetch_historical_data(asset: str, from_time: datetime, to_time: datetime, max_retries: int = 3, retry_delay: float = 1.0) -> List[Dict[str, Any]]:
    """
    Fetch historical miner data from API for a given time range.
    
    Args:
        asset: Asset symbol (e.g., 'BTC', 'ETH')
        from_time: Start time (UTC)
        to_time: End time (UTC)
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
        
    Returns:
        List of data records
    """
    from_str = from_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_str = to_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    params = {
        "from": from_str,
        "to": to_str,
        "asset": asset
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(API_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if not isinstance(data, list):
                print(f"⚠️  Unexpected response format: {type(data)}")
                return []
            
            print(f"✅ Fetched {len(data)} records for {asset} from {from_str} to {to_str}")
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"⚠️  Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
            else:
                print(f"❌ Failed to fetch data after {max_retries} attempts")
                return []
        except Exception as e:
            print(f"❌ Error processing response: {e}")
            return []
    
    return []

def fetch_historical_data_range(asset: str, from_time: datetime, to_time: datetime, chunk_days: int = MAX_DAYS_PER_REQUEST) -> pd.DataFrame:
    """
    Fetch historical data for a time range, handling API limits by splitting into chunks.
    
    Args:
        asset: Asset symbol
        from_time: Start time
        to_time: End time
        chunk_days: Maximum days per request (default: 6)
        
    Returns:
        DataFrame with all fetched records
    """
    all_data = []
    current_time = from_time
    
    print(f"📥 Fetching historical data for {asset} from {from_time.isoformat()} to {to_time.isoformat()}")
    while current_time < to_time:
        chunk_end = min(current_time + timedelta(days=chunk_days), to_time)
        
        print(f"   Fetching chunk: {current_time.isoformat()} to {chunk_end.isoformat()}")
        chunk_data = fetch_historical_data(asset, current_time, chunk_end)
        
        if chunk_data:
            all_data.extend(chunk_data)
            time.sleep(0.5)
        
        current_time = chunk_end
    
    if not all_data:
        print("⚠️  No data fetched")
        return pd.DataFrame()
    
    df = pd.DataFrame(all_data)
    if df.empty:
        return df
    
    # Convert scored_time to datetime
    if 'scored_time' in df.columns:
        df['scored_time'] = pd.to_datetime(df['scored_time'])
    
    # Remove duplicates and sort by scored_time
    df = df.drop_duplicates(subset=['miner_uid', 'asset', 'scored_time'], keep='last')  
    df = df.sort_values('scored_time').reset_index(drop=True)
    
    print(f"✅ Total records fetched (after deduplication): {len(df)}")
    return df

def get_existing_data(filepath: str, save_cleaned: bool = True) -> Optional[pd.DataFrame]:
    """
    Load existing data from file with removing any duplicates.
    """
    if not os.path.exists(filepath):
        return None
    
    try:
        df = pd.read_csv(filepath)
        if df.empty:
            return df
        
        if 'scored_time' in df.columns:
            df['scored_time'] = pd.to_datetime(df['scored_time'])
        
        df = df.drop_duplicates(subset=['miner_uid', 'asset', 'scored_time'], keep='last') # Remove duplicates from existing data
        df = df.sort_values('scored_time').reset_index(drop=True)
        print(f"✅ Loaded {len(df)} existing records from {filepath}")
        return df
    except Exception as e:
        print(f"⚠️  Error loading existing file: {e}")
        return None

def merge_and_save_data(existing_df: Optional[pd.DataFrame], new_df: pd.DataFrame, filepath: str) -> pd.DataFrame:
    """
    Merge existing and new data, removing duplicates, and save to file.
    
    Args:
        existing_df: Existing data DataFrame (None if no existing file)
        new_df: New data DataFrame
        filepath: Path to save merged data
        
    Returns:
        Merged DataFrame
    """
    if existing_df is None or existing_df.empty:
        merged_df = new_df.copy()
    else:
        # Combine dataframes
        before_merge_count = len(existing_df) + len(new_df)
        merged_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Remove duplicates based on miner_uid, asset, and scored_time
        # Keep the last occurrence (most recent data)
        initial_merged_count = len(merged_df)
        merged_df = merged_df.drop_duplicates(
            subset=['miner_uid', 'asset', 'scored_time'],
            keep='last'
        )
        duplicates_removed = initial_merged_count - len(merged_df)
        
        # Sort by scored_time
        merged_df = merged_df.sort_values('scored_time').reset_index(drop=True)
        
        print(f"✅ Merged data: {len(existing_df)} existing + {len(new_df)} new = {before_merge_count} before deduplication")
        if duplicates_removed > 0:
            print(f"   ⚠️  Removed {duplicates_removed} duplicate(s) during merge")
        print(f"   Final count: {len(merged_df)} records")
    
    # Save to CSV
    merged_df.to_csv(filepath, index=False)
    print(f"💾 Saved {len(merged_df)} records to {filepath}")
    
    return merged_df

def fetch_and_update_historical_data(
    asset: str,
    start_time: Optional[datetime] = None,
    output_file: Optional[str] = None,
    force_refresh: bool = False
) -> pd.DataFrame:
    """
    Fetch and update historical miner data.
    
    Args:
        asset: Asset symbol (e.g., 'BTC', 'ETH')
        start_time: Start time for fetching (if None and no existing file, uses default)
        output_file: Output file path (if None, uses default location)
        force_refresh: If True, refetch all data even if file exists
        
    Returns:
        DataFrame with all historical data
    """
    # Determine output file path
    if output_file is None:
        output_file = os.path.join(current_dir, "data", f"historical_{asset.lower()}_data.csv")
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Load existing data
    existing_df = None if force_refresh else get_existing_data(output_file)
    
    # Determine time range for fetching
    if existing_df is None or existing_df.empty:
        if start_time is None:
            from_time = datetime.now(timezone.utc) - timedelta(days=30) # Fetch last 30 days as default
        else:
            from_time = start_time
        
        to_time = datetime.now(timezone.utc)
        print(f"📥 No existing data found. Fetching from {from_time.isoformat()} to {to_time.isoformat()}")
    else:
        # Existing data found - fetch from last timestamp to now
        last_time = existing_df['scored_time'].max()        
        from_time = pd.to_datetime(last_time).to_pydatetime()
        if from_time.tzinfo is None:
            from_time = from_time.replace(tzinfo=timezone.utc)
        
        to_time = datetime.now(timezone.utc)
        print(f"📥 Existing data found. Fetching new data from {from_time.isoformat()} to {to_time.isoformat()}")
    
    # Fetch new data
    new_df = fetch_historical_data_range(asset, from_time, to_time)
    
    if new_df.empty:
        print("⚠️  No new data fetched")
        return existing_df if existing_df is not None else pd.DataFrame()
    
    # Merge and save
    merged_df = merge_and_save_data(existing_df, new_df, output_file)
    
    return merged_df


def main():
    parser = argparse.ArgumentParser(description="Fetch historical miner data from Synth API")
    parser.add_argument("--asset", type=str, default="BTC", choices=["BTC", "ETH", "XAU", "SOL"], help="Asset symbol")
    parser.add_argument("--start-time", type=str, help="Start time for fetching (ISO format, e.g., '2024-01-01T00:00:00Z'). If not provided, uses last 30 days.")
    parser.add_argument("--output-file", type=str, help="Output CSV file path (default: data/historical_{asset}_data.csv)")
    parser.add_argument("--force-refresh", action="store_true", help="Force refresh all data (ignore existing file)")
    
    args = parser.parse_args()
    
    # Parse start_time if provided
    start_time = None
    if args.start_time:
        try:
            start_time = datetime.fromisoformat(args.start_time.replace('Z', '+00:00'))
        except ValueError:
            print(f"❌ Invalid start_time format: {args.start_time}")
            print("   Expected format: '2024-01-01T00:00:00Z'")
            return
    
    print(f"{'='*30}FETCHING HISTORICAL MINER DATA for {args.asset}{'='*30}\n")
    
    # Fetch and update data
    df = fetch_and_update_historical_data(
        asset=args.asset,
        start_time=start_time,
        output_file=args.output_file,
        force_refresh=args.force_refresh
    )
    
    if df.empty:
        print("⚠️  No data available")
        return
    
    # Print summary
    print(f"\n{'='*30}DATA SUMMARY{'='*30}")
    print(f"Total Records: {len(df)}")
    print(f"Time Range: {df['scored_time'].min()} to {df['scored_time'].max()}")
    print(f"Unique Miners: {df['miner_uid'].nunique()}")
    print(f"Unique Time Points: {df['scored_time'].nunique()}")
    print(f"CRPS Range: {df['crps'].min():.2f} to {df['crps'].max():.2f}")
    print(f"Valid CRPS Scores: {(df['crps'] > 0).sum()} ({(df['crps'] > 0).sum() / len(df) * 100:.1f}%)")
    
    # Check for potential duplicates (should be 0 after deduplication)
    potential_duplicates = df.duplicated(subset=['miner_uid', 'asset', 'scored_time']).sum()
    if potential_duplicates > 0:
        print(f"⚠️  Warning: {potential_duplicates} potential duplicate(s) still detected!")
    else:
        print(f"✅ No duplicates detected (all records are unique by miner_uid + asset + scored_time)")
    print(f"{'='*60}\n")
    
    print("✅ Done!")


if __name__ == "__main__":
    main()
