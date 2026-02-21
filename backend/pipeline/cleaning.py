"""
Data cleaning pipeline for Polymarket dataset.

Transforms raw Polymarket API responses into clean, normalized tables:
- events_clean: One row per event
- markets_clean: One row per market with event_id foreign key
- outcomes_clean: Normalized outcomes table (market_id, outcome_name, price, token_id)
"""

import json
import re
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict


def normalize_text(text: Optional[str]) -> Tuple[str, str]:
    """
    Normalize text for similarity/LLM work.
    Returns (raw, cleaned) tuple.
    """
    if not text:
        return ("", "")
    
    raw = text
    # Trim, lowercase, collapse whitespace
    cleaned = re.sub(r'\s+', ' ', text.strip().lower())
    return (raw, cleaned)


def parse_json_field(field: Any, default: Any = None) -> Any:
    """Parse JSON string field, return default if parsing fails."""
    if field is None:
        return default
    if isinstance(field, str):
        try:
            return json.loads(field)
        except (json.JSONDecodeError, TypeError):
            return default
    return field


def parse_date(date_str: Optional[str]) -> Tuple[Optional[datetime], Optional[str]]:
    """
    Parse ISO date string into timezone-aware datetime and YYYY-MM-DD string.
    Returns (datetime_obj, date_string) or (None, None) if invalid.
    """
    if not date_str:
        return (None, None)
    
    try:
        # Try parsing ISO format
        dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        date_day = dt.strftime('%Y-%m-%d')
        return (dt, date_day)
    except (ValueError, AttributeError):
        return (None, None)


def validate_prices(outcomes: List[str], prices: List[Any]) -> Tuple[bool, Dict[str, float]]:
    """
    Validate outcomes and prices, convert to normalized dict.
    Returns (is_valid, outcome_price_dict).
    """
    if not outcomes or not prices:
        return (False, {})
    
    if len(outcomes) != len(prices):
        return (False, {})
    
    outcome_prices = {}
    for outcome, price in zip(outcomes, prices):
        try:
            price_float = float(price)
            # Reject NaN, negatives, >1
            if price_float != price_float or price_float < 0 or price_float > 1:
                return (False, {})
            outcome_prices[outcome] = price_float
        except (ValueError, TypeError):
            return (False, {})
    
    return (True, outcome_prices)


def clean_events(raw_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Clean and normalize events into canonical events table.
    Returns list of cleaned event records.
    """
    events_clean = []
    
    for event in raw_events:
        event_id = event.get('id')
        if not event_id:
            continue
        
        # Parse dates
        start_dt, start_day = parse_date(event.get('startDate'))
        end_dt, end_day = parse_date(event.get('endDate'))
        
        # Normalize text fields
        title_raw, title_clean = normalize_text(event.get('title'))
        desc_raw, desc_clean = normalize_text(event.get('description'))
        
        event_clean = {
            'event_id': event_id,
            'slug': event.get('slug', ''),
            'title_raw': title_raw,
            'title_clean': title_clean,
            'description_raw': desc_raw,
            'description_clean': desc_clean,
            'image': event.get('image', ''),
            'active': event.get('active', False),
            'closed': event.get('closed', False),
            'start_date_iso': event.get('startDate'),
            'start_datetime': start_dt.isoformat() if start_dt else None,
            'start_day': start_day,
            'end_date_iso': event.get('endDate'),
            'end_datetime': end_dt.isoformat() if end_dt else None,
            'end_day': end_day,
            'volume': event.get('volume', 0),
            'volume24hr': event.get('volume24hr', 0),
            'liquidity': event.get('liquidity', 0),
            'tags': event.get('tags', []),
            'series': event.get('series'),
        }
        
        events_clean.append(event_clean)
    
    return events_clean


def extract_markets_from_events(raw_events: List[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], str]]:
    """
    Extract markets from nested events structure.
    Returns list of (market_dict, event_id) tuples.
    """
    markets_with_events = []
    for event in raw_events:
        event_id = event.get('id')
        if not event_id:
            continue
        
        nested_markets = event.get('markets', [])
        for market in nested_markets:
            markets_with_events.append((market, event_id))
    
    return markets_with_events


def clean_markets(
    raw_markets: List[Dict[str, Any]],
    events_clean: List[Dict[str, Any]],
    raw_events: List[Dict[str, Any]],
    min_liquidity: float = 0.0,
    min_prob_sum: float = 0.5,
    max_prob_sum: float = 1.5
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Clean and normalize markets into canonical markets table.
    Also returns outcomes table.
    
    Processes markets from:
    1. Nested markets within events (primary source, has event_id)
    2. Top-level markets array (secondary, tries to match to events)
    
    Filters:
    - Only enableOrderBook == True
    - Only active == True
    - Only closed == False
    - Valid outcomes and prices
    - Liquidity >= min_liquidity
    - Probability sum within [min_prob_sum, max_prob_sum]
    
    Returns (markets_clean, outcomes_clean).
    """
    markets_clean = []
    outcomes_clean = []
    
    # Create event_id lookup
    event_id_map = {e['event_id']: e for e in events_clean}
    
    # Track seen markets for deduplication
    seen_market_ids = set()
    seen_condition_ids = set()
    seen_question_ids = set()
    
    # First, extract markets from nested events (primary source)
    nested_markets_with_events = extract_markets_from_events(raw_events)
    print(f"Found {len(nested_markets_with_events)} markets nested in events")
    
    # Process nested markets
    all_markets_to_process = []
    for market, event_id in nested_markets_with_events:
        all_markets_to_process.append((market, event_id))
    
    # Also process top-level markets (try to match to events if possible)
    # Create a mapping from market condition/question IDs to event IDs if available
    market_to_event_map = {}
    for market, event_id in nested_markets_with_events:
        market_id = market.get('id')
        condition_id = market.get('conditionId')
        question_id = market.get('questionId')
        if market_id:
            market_to_event_map[market_id] = event_id
        if condition_id:
            market_to_event_map[condition_id] = event_id
        if question_id:
            market_to_event_map[question_id] = event_id
    
    # Add top-level markets (only if we can find their event_id)
    for market in raw_markets:
        market_id = market.get('id')
        condition_id = market.get('conditionId')
        question_id = market.get('questionId')
        
        # Try to find event_id from our mapping
        event_id = None
        if market_id and market_id in market_to_event_map:
            event_id = market_to_event_map[market_id]
        elif condition_id and condition_id in market_to_event_map:
            event_id = market_to_event_map[condition_id]
        elif question_id and question_id in market_to_event_map:
            event_id = market_to_event_map[question_id]
        
        # Only include if we found an event_id (to avoid orphaned markets)
        if event_id:
            all_markets_to_process.append((market, event_id))
    
    print(f"Processing {len(all_markets_to_process)} total markets (nested + matched top-level)")
    
    for market, event_id in all_markets_to_process:
        # Filter: only tradable markets
        if not market.get('enableOrderBook', False):
            continue
        if not market.get('active', True):
            continue
        if market.get('closed', False):
            continue
        
        # Get market identifiers
        market_id = market.get('id')
        condition_id = market.get('conditionId')
        question_id = market.get('questionId')
        
        # Deduplicate: use market_id as primary key, fallback to condition_id or question_id
        dedup_key = market_id or condition_id or question_id
        if not dedup_key:
            continue
        
        if dedup_key in seen_market_ids:
            continue
        if condition_id and condition_id in seen_condition_ids:
            continue
        if question_id and question_id in seen_question_ids:
            continue
        
        seen_market_ids.add(dedup_key)
        if condition_id:
            seen_condition_ids.add(condition_id)
        if question_id:
            seen_question_ids.add(question_id)
        
        # Parse JSON fields
        outcomes = parse_json_field(market.get('outcomes'), [])
        outcome_prices = parse_json_field(market.get('outcomePrices'), [])
        clob_token_ids = parse_json_field(market.get('clobTokenIds'), {})
        
        # Validate outcomes and prices
        if not outcomes or not outcome_prices:
            continue
        
        is_valid, outcome_price_dict = validate_prices(outcomes, outcome_prices)
        if not is_valid:
            continue
        
        # Check probability sum
        prob_sum = sum(outcome_price_dict.values())
        if prob_sum < min_prob_sum or prob_sum > max_prob_sum:
            continue
        
        # Filter by liquidity (convert to float if needed)
        liquidity_raw = market.get('liquidity', 0)
        try:
            liquidity = float(liquidity_raw) if liquidity_raw is not None else 0.0
        except (ValueError, TypeError):
            liquidity = 0.0
        if liquidity < min_liquidity:
            continue
        
        # event_id is already provided from the tuple
        # Verify it exists in our event map
        if event_id not in event_id_map:
            continue
        
        # Parse dates
        end_dt, end_day = parse_date(market.get('endDate'))
        
        # Normalize text fields
        question_raw, question_clean = normalize_text(market.get('question'))
        
        # Create clean market record
        market_clean = {
            'market_id': market_id,
            'condition_id': condition_id,
            'question_id': question_id,
            'event_id': event_id,  # Foreign key
            'slug': market.get('slug', ''),
            'question_raw': question_raw,
            'question_clean': question_clean,
            'market_maker_address': market.get('marketMakerAddress', ''),
            'num_outcomes': len(outcomes),
            'probability_sum': prob_sum,
            'active': market.get('active', True),
            'closed': market.get('closed', False),
            'volume': market.get('volume', 0),
            'volume24hr': market.get('volume24hr', 0),
            'liquidity': liquidity,
            'end_date_iso': market.get('endDate'),
            'end_datetime': end_dt.isoformat() if end_dt else None,
            'end_day': end_day,
        }
        
        markets_clean.append(market_clean)
        
        # Create outcomes records
        for outcome_name, price in outcome_price_dict.items():
            token_id = clob_token_ids.get(outcome_name) if isinstance(clob_token_ids, dict) else None
            
            outcome_record = {
                'market_id': market_id,
                'outcome_name': outcome_name,
                'price': price,
                'token_id': token_id,
            }
            outcomes_clean.append(outcome_record)
    
    return markets_clean, outcomes_clean


def load_dataset(filepath: str) -> Dict[str, Any]:
    """Load the raw dataset JSON file."""
    print(f"Loading dataset from {filepath}...")
    
    # Try standard json.load first
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"Loaded: {len(data.get('events', []))} events, {len(data.get('markets', []))} markets")
        return data
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        print("Attempting to load with streaming parser (ijson)...")
        
        # Fallback to ijson for streaming parsing
        try:
            import ijson  # type: ignore
            data = {'events': [], 'markets': [], 'total_events': 0, 'total_markets': 0}
            
            with open(filepath, 'rb') as f:
                # Parse events
                events_parser = ijson.items(f, 'events.item')
                for event in events_parser:
                    data['events'].append(event)
                data['total_events'] = len(data['events'])
                
                # Reset file pointer for markets
                f.seek(0)
                markets_parser = ijson.items(f, 'markets.item')
                for market in markets_parser:
                    data['markets'].append(market)
                data['total_markets'] = len(data['markets'])
            
            print(f"Loaded with ijson: {data['total_events']} events, {data['total_markets']} markets")
            return data
        except ImportError:
            print("ERROR: ijson not installed. Install with: pip install ijson")
            print("Alternatively, the JSON file may be corrupted.")
            raise
        except Exception as e2:
            print(f"Error with ijson: {e2}")
            print("The JSON file may be corrupted. Please check the file.")
            raise


def save_clean_dataset(
    events_clean: List[Dict[str, Any]],
    markets_clean: List[Dict[str, Any]],
    outcomes_clean: List[Dict[str, Any]],
    output_dir: str = "."
) -> None:
    """Save cleaned datasets to JSON files."""
    print(f"\nSaving cleaned datasets...")
    
    events_path = f"{output_dir}/events_clean.json"
    markets_path = f"{output_dir}/markets_clean.json"
    outcomes_path = f"{output_dir}/outcomes_clean.json"
    
    with open(events_path, 'w', encoding='utf-8') as f:
        json.dump(events_clean, f, indent=2, ensure_ascii=False, default=str)
    print(f"Saved {len(events_clean)} events to {events_path}")
    
    with open(markets_path, 'w', encoding='utf-8') as f:
        json.dump(markets_clean, f, indent=2, ensure_ascii=False, default=str)
    print(f"Saved {len(markets_clean)} markets to {markets_path}")
    
    with open(outcomes_path, 'w', encoding='utf-8') as f:
        json.dump(outcomes_clean, f, indent=2, ensure_ascii=False, default=str)
    print(f"Saved {len(outcomes_clean)} outcomes to {outcomes_path}")
    
    # Print summary statistics
    print(f"\n=== Cleaning Summary ===")
    print(f"Events: {len(events_clean)}")
    print(f"Markets: {len(markets_clean)}")
    print(f"Outcomes: {len(outcomes_clean)}")
    print(f"Avg outcomes per market: {len(outcomes_clean) / len(markets_clean) if markets_clean else 0:.2f}")


def main(
    input_file: str = "polymarket_dataset.json",
    output_dir: str = ".",
    min_liquidity: float = 0.0,
    min_prob_sum: float = 0.5,
    max_prob_sum: float = 1.5
):
    """
    Main cleaning pipeline.
    
    Args:
        input_file: Path to raw dataset JSON
        output_dir: Directory to save cleaned outputs
        min_liquidity: Minimum liquidity threshold (default: 0.0, no filter)
        min_prob_sum: Minimum probability sum (default: 0.5)
        max_prob_sum: Maximum probability sum (default: 1.5)
    """
    # Load raw dataset
    data = load_dataset(input_file)
    
    raw_events = data.get('events', [])
    raw_markets = data.get('markets', [])
    
    print(f"\nProcessing {len(raw_events)} events and {len(raw_markets)} markets...")
    
    # Clean events
    print("\nCleaning events...")
    events_clean = clean_events(raw_events)
    print(f"Cleaned {len(events_clean)} events")
    
    # Clean markets
    print("\nCleaning markets...")
    markets_clean, outcomes_clean = clean_markets(
        raw_markets,
        events_clean,
        raw_events,
        min_liquidity=min_liquidity,
        min_prob_sum=min_prob_sum,
        max_prob_sum=max_prob_sum
    )
    print(f"Cleaned {len(markets_clean)} markets, {len(outcomes_clean)} outcomes")
    
    # Save cleaned datasets
    save_clean_dataset(events_clean, markets_clean, outcomes_clean, output_dir)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Clean Polymarket dataset")
    parser.add_argument(
        "--input",
        default="polymarket_dataset.json",
        help="Input dataset file (default: polymarket_dataset.json)"
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Output directory for cleaned files (default: current directory)"
    )
    parser.add_argument(
        "--min-liquidity",
        type=float,
        default=0.0,
        help="Minimum liquidity threshold (default: 0.0)"
    )
    parser.add_argument(
        "--min-prob-sum",
        type=float,
        default=0.5,
        help="Minimum probability sum (default: 0.5)"
    )
    parser.add_argument(
        "--max-prob-sum",
        type=float,
        default=1.5,
        help="Maximum probability sum (default: 1.5)"
    )
    
    args = parser.parse_args()
    
    main(
        input_file=args.input,
        output_dir=args.output_dir,
        min_liquidity=args.min_liquidity,
        min_prob_sum=args.min_prob_sum,
        max_prob_sum=args.max_prob_sum
    )
