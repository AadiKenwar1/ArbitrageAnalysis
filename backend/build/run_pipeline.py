"""
Master Pipeline Script

Runs the complete arbitrage detection pipeline:
1. Ingest data (optional)
2. Clean data
3. Detect dependencies
4. Find arbitrage opportunities
"""

import subprocess
import sys
import argparse
import os
from pathlib import Path


def run_command(cmd, description, required=True):
    """Run a command and handle errors."""
    print("\n" + "=" * 60)
    print(f"Step: {description}")
    print("=" * 60)
    print(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("Warnings/Errors:", result.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: {description} failed!")
        print(f"Exit code: {e.returncode}")
        print(f"Error output: {e.stderr}")
        if required:
            print(f"\nPipeline stopped. Fix the error and try again.")
            sys.exit(1)
        return False
    except FileNotFoundError:
        print(f"ERROR: Command not found. Make sure Python is in your PATH.")
        if required:
            sys.exit(1)
        return False


def check_file_exists(filepath, description):
    """Check if a required file exists."""
    if not os.path.exists(filepath):
        print(f"WARNING: {description} not found: {filepath}")
        return False
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Run the complete arbitrage detection pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline (fetch fresh data)
  python run_pipeline.py --fetch
  
  # Run pipeline with existing data (skip ingestion)
  python run_pipeline.py
  
  # Run with custom thresholds
  python run_pipeline.py --min-profit 0.001 --fee-rate 0.01
  
  # Skip cleaning if already done
  python run_pipeline.py --skip-clean --skip-dependencies
        """
    )
    
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Fetch fresh data from API (runs ingestor)"
    )
    parser.add_argument(
        "--skip-clean",
        action="store_true",
        help="Skip data cleaning step"
    )
    parser.add_argument(
        "--skip-dependencies",
        action="store_true",
        help="Skip dependency detection step"
    )
    parser.add_argument(
        "--min-profit",
        type=float,
        default=0.001,
        help="Minimum profit threshold for arbitrage (default: 0.001 = 0.1%%)"
    )
    parser.add_argument(
        "--fee-rate",
        type=float,
        default=0.02,
        help="Trading fee rate (default: 0.02 = 2%%)"
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Number of top opportunities to display (default: 20)"
    )
    parser.add_argument(
        "--input-dataset",
        default="polymarket_dataset.json",
        help="Input dataset file (default: polymarket_dataset.json)"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Arbitrage Detection Pipeline")
    print("=" * 60)
    print(f"Configuration:")
    print(f"  Fetch fresh data: {args.fetch}")
    print(f"  Skip cleaning: {args.skip_clean}")
    print(f"  Skip dependencies: {args.skip_dependencies}")
    print(f"  Min profit threshold: {args.min_profit*100:.2f}%%")
    print(f"  Fee rate: {args.fee_rate*100:.2f}%%")
    print("=" * 60)
    
    # Step 1: Ingest data (optional)
    if args.fetch:
        run_command(
            ["python", "backend/ingestor.py"],
            "Fetching data from Polymarket API",
            required=False
        )
    else:
        print("\nSkipping data ingestion (use --fetch to get fresh data)")
        if not check_file_exists(args.input_dataset, "Input dataset"):
            print("ERROR: Input dataset not found. Run with --fetch or provide dataset.")
            sys.exit(1)
    
    # Step 2: Clean data
    if not args.skip_clean:
        clean_cmd = [
            "python", "backend/cleaning.py",
            "--input", args.input_dataset
        ]
        run_command(clean_cmd, "Cleaning and normalizing data")
    else:
        print("\nSkipping data cleaning")
        required_files = ["events_clean.json", "markets_clean.json", "outcomes_clean.json"]
        for f in required_files:
            if not check_file_exists(f, f"Cleaned data file ({f})"):
                print(f"ERROR: Required file {f} not found. Run cleaning step.")
                sys.exit(1)
    
    # Step 3: Detect dependencies
    if not args.skip_dependencies:
        run_command(
            ["python", "backend/dependency_detector.py"],
            "Detecting market dependencies"
        )
    else:
        print("\nSkipping dependency detection")
        if not check_file_exists("dependencies.json", "Dependencies file"):
            print("ERROR: dependencies.json not found. Run dependency detection step.")
            sys.exit(1)
    
    # Step 4: Find arbitrage opportunities
    arbitrage_cmd = [
        "python", "backend/arbitrage_detector.py",
        "--min-profit", str(args.min_profit),
        "--fee-rate", str(args.fee_rate),
        "--top-n", str(args.top_n)
    ]
    run_command(arbitrage_cmd, "Detecting arbitrage opportunities")
    
    # Final summary
    print("\n" + "=" * 60)
    print("Pipeline Complete!")
    print("=" * 60)
    
    # Check if opportunities were found
    if os.path.exists("arbitrage_opportunities.json"):
        import json
        with open("arbitrage_opportunities.json", 'r', encoding='utf-8') as f:
            opps = json.load(f)
        
        total = opps['summary']['total_opportunities']
        if total > 0:
            print(f"\n[SUCCESS] Found {total} arbitrage opportunities!")
            print(f"   - Sell opportunities: {opps['summary']['sell_opportunities']}")
            print(f"   - Buy opportunities: {opps['summary']['buy_opportunities']}")
            print(f"   - Total net profit potential: {opps['summary']['total_net_profit']*100:.2f}%")
            print(f"\n[INFO] Full results saved to: arbitrage_opportunities.json")
        else:
            print(f"\n[WARNING] No arbitrage opportunities found with current thresholds.")
            print(f"   Try lowering --min-profit (current: {args.min_profit*100:.2f}%)")
            print(f"   Or fetch fresh data with --fetch")
    else:
        print("\n[WARNING] arbitrage_opportunities.json not found")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
