"""
Dependency Detection Module

Identifies logically dependent markets that should have price constraints.
Detects:
- Complementary outcomes (Yes/No pairs within same market)
- Mutually exclusive markets (same event, can't both be true)
- Hierarchical relationships (if A then B)
"""

import json
import re
from typing import Dict, List, Any, Set, Tuple, Optional
from collections import defaultdict


def load_clean_data(
    events_path: str = "events_clean.json",
    markets_path: str = "markets_clean.json",
    outcomes_path: str = "outcomes_clean.json"
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """Load cleaned datasets."""
    print(f"Loading cleaned datasets...")
    
    with open(events_path, 'r', encoding='utf-8') as f:
        events = json.load(f)
    
    with open(markets_path, 'r', encoding='utf-8') as f:
        markets = json.load(f)
    
    with open(outcomes_path, 'r', encoding='utf-8') as f:
        outcomes = json.load(f)
    
    print(f"Loaded: {len(events)} events, {len(markets)} markets, {len(outcomes)} outcomes")
    return events, markets, outcomes


def build_market_indexes(
    markets: List[Dict],
    outcomes: List[Dict]
) -> Tuple[Dict[str, Dict], Dict[str, List[Dict]], Dict[str, List[str]]]:
    """
    Build indexes for fast lookup.
    Returns:
    - market_by_id: {market_id: market_dict}
    - markets_by_event: {event_id: [market_dicts]}
    - outcomes_by_market: {market_id: [outcome_names]}
    """
    market_by_id = {m['market_id']: m for m in markets}
    markets_by_event = defaultdict(list)
    outcomes_by_market = defaultdict(list)
    
    for market in markets:
        event_id = market.get('event_id')
        if event_id:
            markets_by_event[event_id].append(market)
    
    for outcome in outcomes:
        market_id = outcome['market_id']
        outcome_name = outcome['outcome_name']
        outcomes_by_market[market_id].append(outcome_name)
    
    return market_by_id, dict(markets_by_event), dict(outcomes_by_market)


def find_complementary_outcomes(
    markets: List[Dict],
    outcomes_by_market: Dict[str, List[str]]
) -> List[Dict[str, Any]]:
    """
    Find markets with complementary outcomes (Yes/No, Win/Lose, etc.).
    These should sum to ~1.0.
    """
    dependencies = []
    
    # Common complementary pairs
    complementary_pairs = [
        ('yes', 'no'),
        ('win', 'lose'),
        ('win', 'loss'),
        ('true', 'false'),
        ('happens', "doesn't happen"),
        ('happens', 'does not happen'),
    ]
    
    for market in markets:
        market_id = market['market_id']
        outcome_names = outcomes_by_market.get(market_id, [])
        
        if len(outcome_names) != 2:
            continue
        
        # Normalize outcome names for comparison
        outcome_lower = [o.lower().strip() for o in outcome_names]
        
        # Check if it's a complementary pair
        is_complementary = False
        for pair in complementary_pairs:
            if (pair[0] in outcome_lower[0] and pair[1] in outcome_lower[1]) or \
               (pair[0] in outcome_lower[1] and pair[1] in outcome_lower[0]):
                is_complementary = True
                break
        
        # Also check if outcomes are exact opposites
        if not is_complementary:
            # Check for negation patterns
            if any(neg in outcome_lower[0] for neg in ['not', "n't", 'no']) and \
               outcome_lower[0].replace('not', '').replace("n't", '').replace('no', '').strip() in outcome_lower[1]:
                is_complementary = True
            elif any(neg in outcome_lower[1] for neg in ['not', "n't", 'no']) and \
                 outcome_lower[1].replace('not', '').replace("n't", '').replace('no', '').strip() in outcome_lower[0]:
                is_complementary = True
        
        if is_complementary:
            dependencies.append({
                'type': 'complementary',
                'market_id': market_id,
                'event_id': market.get('event_id'),
                'outcomes': outcome_names,
                'constraint': 'sum_should_equal_1.0',
                'question': market.get('question_clean', '')
            })
    
    return dependencies


def extract_entities(text: str) -> Dict[str, List[str]]:
    """
    Extract entities from text (teams, candidates, dates, numbers).
    Returns dict with entity types and values.
    """
    entities = {
        'teams': [],
        'candidates': [],
        'dates': [],
        'numbers': [],
        'keywords': []
    }
    
    if not text:
        return entities
    
    # Extract years (4-digit numbers)
    years = re.findall(r'\b(19|20)\d{2}\b', text)
    entities['dates'].extend(years)
    
    # Extract numbers
    numbers = re.findall(r'\b\d+\.?\d*\b', text)
    entities['numbers'].extend(numbers)
    
    # Common team/candidate patterns (capitalized words, proper nouns)
    # This is simplified - could be enhanced with NER
    capitalized = re.findall(r'\b[A-Z][a-z]+\b', text)
    entities['keywords'].extend(capitalized)
    
    return entities


def find_mutually_exclusive_markets(
    markets_by_event: Dict[str, List[Dict]],
    outcomes_by_market: Dict[str, List[str]],
    market_by_id: Dict[str, Dict]
) -> List[Dict[str, Any]]:
    """
    Find markets in the same event with mutually exclusive outcomes.
    
    Based on paper methodology: markets in same event with same end date
    that have "Yes" outcomes are potentially mutually exclusive.
    
    Example: "Team A wins" and "Team B wins" in same game.
    """
    dependencies = []
    
    for event_id, event_markets in markets_by_event.items():
        if len(event_markets) < 2:
            continue
        
        # Group markets by end_day for better matching (per paper methodology)
        markets_by_end_day = defaultdict(list)
        for market in event_markets:
            end_day = market.get('end_day')
            if end_day:
                markets_by_end_day[end_day].append(market)
        
        # Check markets within same event and same end day
        for end_day, day_markets in markets_by_end_day.items():
            if len(day_markets) < 2:
                continue
            
            for i, market_a in enumerate(day_markets):
                for market_b in day_markets[i+1:]:
                    market_a_id = market_a['market_id']
                    market_b_id = market_b['market_id']
                    
                    outcomes_a = outcomes_by_market.get(market_a_id, [])
                    outcomes_b = outcomes_by_market.get(market_b_id, [])
                    
                    if not outcomes_a or not outcomes_b:
                        continue
                    
                    # Simplified heuristic: Both markets have "Yes" outcomes
                    # and are in same event with same end date
                    # This is a conservative approach - can be refined with LLM later
                    outcomes_a_lower = [o.lower() for o in outcomes_a]
                    outcomes_b_lower = [o.lower() for o in outcomes_b]
                    
                    # Check if both have "Yes" outcomes (most common case)
                    if 'yes' in outcomes_a_lower and 'yes' in outcomes_b_lower:
                        question_a = market_a.get('question_clean', '')
                        question_b = market_b.get('question_clean', '')
                        
                        # Additional check: questions should be different
                        # (avoid matching same market to itself)
                        if question_a != question_b:
                            # Extract what the "Yes" outcome represents from each question
                            # For "Will X happen?" → Yes means "X happens"
                            # We need to check if "X happens" in A and "Y happens" in B are mutually exclusive
                            
                            # Simple heuristic: Check if questions are about the same type of outcome
                            # but with different subjects (e.g., "Team A wins" vs "Team B wins")
                            
                            # Extract the subject/entity from each question
                            # Pattern: "will [subject] [action]?" or "[subject] [action]?"
                            subject_pattern = r'will\s+([^?]+?)\s+(?:happen|occur|win|qualify|get|be|do)'
                            subject_a_match = re.search(subject_pattern, question_a, re.IGNORECASE)
                            subject_b_match = re.search(subject_pattern, question_b, re.IGNORECASE)
                            
                            # Alternative: extract capitalized entities (proper nouns)
                            entities_a = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b', question_a)
                            entities_b = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b', question_b)
                            
                            # Check if they have the same action/verb but different subjects
                            # This suggests mutually exclusive outcomes
                            action_words = ['win', 'wins', 'qualify', 'qualifies', 'get', 'gets', 'be', 'is']
                            has_same_action = any(action in question_a.lower() and action in question_b.lower() 
                                                 for action in action_words)
                            
                            # Check if subjects are different
                            subjects_different = True
                            if subject_a_match and subject_b_match:
                                subject_a = subject_a_match.group(1).lower().strip()
                                subject_b = subject_b_match.group(1).lower().strip()
                                # If subjects are the same, not mutually exclusive
                                if subject_a == subject_b:
                                    subjects_different = False
                            
                            # Only add if they have same action but different subjects
                            # This is a heuristic - can be improved with LLM
                            if has_same_action and subjects_different:
                                dependencies.append({
                                    'type': 'mutually_exclusive',
                                    'markets': [market_a_id, market_b_id],
                                    'event_id': event_id,
                                    'end_day': end_day,
                                    'outcomes': {
                                        market_a_id: outcomes_a,
                                        market_b_id: outcomes_b
                                    },
                                    'constraint': 'sum_should_be_leq_1.0',
                                    'questions': {
                                        market_a_id: question_a,
                                        market_b_id: question_b
                                    },
                                    'subset_a': ['Yes'],  # "Yes" in market A
                                    'subset_b': ['Yes'],  # "Yes" in market B
                                    'outcome_a_meaning': question_a,  # What "Yes" means in A
                                    'outcome_b_meaning': question_b   # What "Yes" means in B
                                })
                    
                    # Also check for explicit winner patterns in outcomes
                    outcomes_a_str = ' '.join(outcomes_a).lower()
                    outcomes_b_str = ' '.join(outcomes_b).lower()
                    
                    winner_pattern = r'(\w+)\s+(wins?|winner)'
                    winner_a = re.findall(winner_pattern, outcomes_a_str)
                    winner_b = re.findall(winner_pattern, outcomes_b_str)
                    
                    if winner_a and winner_b:
                        # Different winners in same event = mutually exclusive
                        if winner_a[0][0] != winner_b[0][0]:
                            question_a = market_a.get('question_clean', '')
                            question_b = market_b.get('question_clean', '')
                            
                            dependencies.append({
                                'type': 'mutually_exclusive',
                                'markets': [market_a_id, market_b_id],
                                'event_id': event_id,
                                'end_day': market_a.get('end_day'),
                                'outcomes': {
                                    market_a_id: outcomes_a,
                                    market_b_id: outcomes_b
                                },
                                'constraint': 'sum_should_be_leq_1.0',
                                'questions': {
                                    market_a_id: question_a,
                                    market_b_id: question_b
                                },
                                'subset_a': [winner_a[0][0] + ' wins'],
                                'subset_b': [winner_b[0][0] + ' wins']
                            })
    
    return dependencies


def find_hierarchical_relationships(
    markets_by_event: Dict[str, List[Dict]],
    outcomes_by_market: Dict[str, List[str]]
) -> List[Dict[str, Any]]:
    """
    Find hierarchical relationships (if A then B).
    Example: "Win by >10" implies "Win"
    """
    dependencies = []
    
    # This is a simplified version - could be enhanced with semantic analysis
    for event_id, event_markets in markets_by_event.items():
        if len(event_markets) < 2:
            continue
        
        for i, market_a in enumerate(event_markets):
            for market_b in event_markets[i+1:]:
                question_a = market_a.get('question_clean', '')
                question_b = market_b.get('question_clean', '')
                
                # Pattern: "X by >N" implies "X"
                # Example: "Win by >10" implies "Win"
                pattern_a = re.search(r'(\w+)\s+by\s+[><=]', question_a)
                pattern_b = re.search(r'(\w+)\s+by\s+[><=]', question_b)
                
                if pattern_a:
                    base_action = pattern_a.group(1)
                    if base_action in question_b.lower():
                        dependencies.append({
                            'type': 'hierarchical',
                            'parent_market_id': market_b['market_id'],
                            'child_market_id': market_a['market_id'],
                            'event_id': event_id,
                            'constraint': 'P(child) <= P(parent)',
                            'relationship': f"{question_a} implies {question_b}"
                        })
    
    return dependencies


def detect_all_dependencies(
    events_path: str = "events_clean.json",
    markets_path: str = "markets_clean.json",
    outcomes_path: str = "outcomes_clean.json"
) -> Dict[str, Any]:
    """
    Main function to detect all dependencies.
    Returns a dictionary with all detected relationships.
    """
    print("=" * 60)
    print("Dependency Detection")
    print("=" * 60)
    
    # Load data
    events, markets, outcomes = load_clean_data(events_path, markets_path, outcomes_path)
    
    # Build indexes
    print("\nBuilding indexes...")
    market_by_id, markets_by_event, outcomes_by_market = build_market_indexes(markets, outcomes)
    print(f"Indexed {len(markets_by_event)} events with markets")
    
    # Find complementary outcomes (Yes/No pairs)
    print("\nFinding complementary outcomes...")
    complementary = find_complementary_outcomes(markets, outcomes_by_market)
    print(f"Found {len(complementary)} markets with complementary outcomes")
    
    # Find mutually exclusive markets
    print("\nFinding mutually exclusive markets...")
    mutually_exclusive = find_mutually_exclusive_markets(
        markets_by_event, outcomes_by_market, market_by_id
    )
    print(f"Found {len(mutually_exclusive)} pairs of mutually exclusive markets")
    
    # Find hierarchical relationships
    print("\nFinding hierarchical relationships...")
    hierarchical = find_hierarchical_relationships(markets_by_event, outcomes_by_market)
    print(f"Found {len(hierarchical)} hierarchical relationships")
    
    # Compile results
    results = {
        'summary': {
            'total_events': len(events),
            'total_markets': len(markets),
            'total_outcomes': len(outcomes),
            'complementary_dependencies': len(complementary),
            'mutually_exclusive_dependencies': len(mutually_exclusive),
            'hierarchical_dependencies': len(hierarchical),
            'total_dependencies': len(complementary) + len(mutually_exclusive) + len(hierarchical)
        },
        'complementary': complementary,
        'mutually_exclusive': mutually_exclusive,
        'hierarchical': hierarchical
    }
    
    return results


def save_dependencies(dependencies: Dict[str, Any], output_path: str = "dependencies.json"):
    """Save dependencies to JSON file."""
    print(f"\nSaving dependencies to {output_path}...")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(dependencies, f, indent=2, ensure_ascii=False, default=str)
    print(f"Saved {dependencies['summary']['total_dependencies']} dependencies")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Detect market dependencies")
    parser.add_argument(
        "--events",
        default="events_clean.json",
        help="Path to events_clean.json"
    )
    parser.add_argument(
        "--markets",
        default="markets_clean.json",
        help="Path to markets_clean.json"
    )
    parser.add_argument(
        "--outcomes",
        default="outcomes_clean.json",
        help="Path to outcomes_clean.json"
    )
    parser.add_argument(
        "--output",
        default="dependencies.json",
        help="Output path for dependencies JSON"
    )
    
    args = parser.parse_args()
    
    dependencies = detect_all_dependencies(
        events_path=args.events,
        markets_path=args.markets,
        outcomes_path=args.outcomes
    )
    
    save_dependencies(dependencies, args.output)
    
    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  Complementary: {dependencies['summary']['complementary_dependencies']}")
    print(f"  Mutually Exclusive: {dependencies['summary']['mutually_exclusive_dependencies']}")
    print(f"  Hierarchical: {dependencies['summary']['hierarchical_dependencies']}")
    print(f"  Total: {dependencies['summary']['total_dependencies']}")
    print("=" * 60)
