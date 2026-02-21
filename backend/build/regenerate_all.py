"""
Regenerate All JSON Files

Regenerates all JSON files in the pipeline:
1. Raw dataset (polymarket_dataset.json)
2. Cleaned data (events_clean.json, markets_clean.json, outcomes_clean.json)
3. Dependencies (dependencies.json)
4. Arbitrage opportunities (arbitrage_opportunities.json)
"""

import subprocess
import sys
import os
from pathlib import Path


def run_command(cmd, description):
    """Run a command and handle errors."""
    print("\n" + "=" * 60)
    print(f"Step: {description}")
    print("=" * 60)
    print(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("Warnings:", result.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: {description} failed!")
        print(f"Exit code: {e.returncode}")
        if e.stderr:
            print(f"Error: {e.stderr}")
        return False
    except FileNotFoundError:
        print(f"ERROR: Command not found. Make sure Python is in your PATH.")
        return False


def main():
    print("=" * 60)
    print("Regenerate All JSON Files")
    print("=" * 60)
    print("\nThis will regenerate:")
    print("  1. Raw dataset (polymarket_dataset.json)")
    print("  2. Cleaned data (events_clean.json, markets_clean.json, outcomes_clean.json)")
    print("  3. Dependencies (dependencies.json)")
    print("  4. Arbitrage opportunities (arbitrage_opportunities.json)")
    print("\nThis may take several minutes...")
    
    # Get the project root (parent of backend/)
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent
    
    # Change to project root
    os.chdir(project_root)
    
    # Step 1: Fetch raw data
    print("\n" + "=" * 60)
    print("Step 1: Fetching raw data from Polymarket API")
    print("=" * 60)
    success = run_command(
        ["python", "backend/pipeline/ingestor.py"],
        "Fetching data from Polymarket API"
    )
    if not success:
        print("ERROR: Failed to fetch data. Aborting.")
        sys.exit(1)
    
    # Step 2: Clean data
    print("\n" + "=" * 60)
    print("Step 2: Cleaning and normalizing data")
    print("=" * 60)
    success = run_command(
        ["python", "backend/pipeline/cleaning.py", "--input", "polymarket_dataset.json"],
        "Cleaning data"
    )
    if not success:
        print("ERROR: Failed to clean data. Aborting.")
        sys.exit(1)
    
    # Step 3: Detect dependencies
    print("\n" + "=" * 60)
    print("Step 3: Detecting market dependencies")
    print("=" * 60)
    success = run_command(
        ["python", "backend/pipeline/dependency_detector.py"],
        "Detecting dependencies"
    )
    if not success:
        print("ERROR: Failed to detect dependencies. Aborting.")
        sys.exit(1)
    
    # Step 4: Find arbitrage opportunities
    print("\n" + "=" * 60)
    print("Step 4: Detecting arbitrage opportunities")
    print("=" * 60)
    success = run_command(
        ["python", "backend/pipeline/arbitrage_detector.py", "--min-profit", "0.00001"],
        "Detecting arbitrage"
    )
    if not success:
        print("ERROR: Failed to detect arbitrage opportunities.")
        sys.exit(1)
    
    # Summary
    print("\n" + "=" * 60)
    print("Regeneration Complete!")
    print("=" * 60)
    
    files = [
        "polymarket_dataset.json",
        "events_clean.json",
        "markets_clean.json",
        "outcomes_clean.json",
        "dependencies.json",
        "arbitrage_opportunities.json"
    ]
    
    print("\nGenerated files:")
    for f in files:
        if os.path.exists(f):
            size_mb = os.path.getsize(f) / (1024 * 1024)
            print(f"  ✓ {f} ({size_mb:.2f} MB)")
        else:
            print(f"  ✗ {f} (not found)")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
