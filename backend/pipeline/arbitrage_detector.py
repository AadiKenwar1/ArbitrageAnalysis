"""
Arbitrage Detection Module

Identifies arbitrage opportunities by checking if prices violate logical constraints.
For complementary outcomes (Yes/No pairs), prices should sum to 1.0.
"""

import json
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict


def load_data(
    dependencies_path: str = "dependencies.json",
    outcomes_path: str = "outcomes_clean.json",
    markets_path: str = "markets_clean.json"
) -> Tuple[Dict, List[Dict], Dict[str, Dict]]:
    """Load dependencies, outcomes, and markets."""
    print("Loading data...")
    
    with open(dependencies_path, 'r', encoding='utf-8') as f:
        dependencies = json.load(f)
    
    with open(outcomes_path, 'r', encoding='utf-8') as f:
        outcomes = json.load(f)
    
    with open(markets_path, 'r', encoding='utf-8') as f:
        markets = json.load(f)
    
    # Build market lookup
    market_by_id = {m['market_id']: m for m in markets}
    
    print(f"Loaded: {len(dependencies.get('complementary', []))} complementary dependencies")
    print(f"Loaded: {len(dependencies.get('mutually_exclusive', []))} mutually exclusive dependencies")
    print(f"Loaded: {len(outcomes)} outcomes")
    print(f"Loaded: {len(markets)} markets")
    
    return dependencies, outcomes, market_by_id


def build_outcome_price_index(outcomes: List[Dict]) -> Dict[str, Dict[str, float]]:
    """
    Build index: {market_id: {outcome_name: price}}
    """
    outcome_prices = defaultdict(dict)
    
    for outcome in outcomes:
        market_id = outcome['market_id']
        outcome_name = outcome['outcome_name']
        price = outcome['price']
        outcome_prices[market_id][outcome_name] = price
    
    return dict(outcome_prices)


def check_complementary_arbitrage(
    dependency: Dict[str, Any],
    outcome_prices: Dict[str, Dict[str, float]],
    market_info: Optional[Dict] = None,
    fee_rate: float = 0.02,  # 2% fee assumption
    min_profit_threshold: float = 0.0001  # 0.01% minimum profit (lowered to catch more opportunities)
) -> Optional[Dict[str, Any]]:
    """
    Check if a complementary dependency has arbitrage opportunity.
    
    For complementary outcomes (Yes/No), prices should sum to 1.0.
    - If sum > 1.0: Sell both outcomes (guaranteed profit)
    - If sum < 1.0: Buy both outcomes (guaranteed profit)
    
    Returns arbitrage opportunity dict or None if no opportunity.
    """
    market_id = dependency['market_id']
    outcome_names = dependency['outcomes']
    
    # Get current prices
    prices = outcome_prices.get(market_id, {})
    
    if len(outcome_names) != 2:
        return None
    
    # Get prices for both outcomes
    price_a = prices.get(outcome_names[0])
    price_b = prices.get(outcome_names[1])
    
    if price_a is None or price_b is None:
        return None
    
    # Calculate sum
    price_sum = price_a + price_b
    
    # Check constraint violation
    expected_sum = 1.0
    violation = price_sum - expected_sum
    
    # If violation is too small, ignore (accounting for rounding)
    # Lowered threshold to catch smaller opportunities (0.0001 = 0.01%)
    if abs(violation) < 0.0001:
        return None
    
    # Calculate profit potential
    if violation > 0:
        # Sum > 1.0: Sell both outcomes
        # You sell at current prices, guaranteed profit = violation amount
        gross_profit = violation
        action = 'sell_both'
        action_detail = f"Sell '{outcome_names[0]}' at {price_a:.4f} and '{outcome_names[1]}' at {price_b:.4f}"
    else:
        # Sum < 1.0: Buy both outcomes
        # You buy at current prices, guaranteed profit = |violation| amount
        gross_profit = abs(violation)
        action = 'buy_both'
        action_detail = f"Buy '{outcome_names[0]}' at {price_a:.4f} and '{outcome_names[1]}' at {price_b:.4f}"
    
    # Account for fees (simplified: assume fee on both transactions)
    # In reality, fees might be different for buy vs sell
    net_profit = gross_profit * (1 - fee_rate)
    
    # Filter by minimum profit threshold
    if net_profit < min_profit_threshold:
        return None
    
    # Build opportunity dict
    opportunity = {
        'market_id': market_id,
        'event_id': dependency.get('event_id'),
        'type': 'complementary',
        'outcomes': {
            outcome_names[0]: price_a,
            outcome_names[1]: price_b
        },
        'outcome_names': outcome_names,
        'price_sum': price_sum,
        'expected_sum': expected_sum,
        'violation': violation,
        'violation_percent': abs(violation) * 100,
        'gross_profit': gross_profit,
        'fee_rate': fee_rate,
        'net_profit': net_profit,
        'net_profit_percent': net_profit * 100,
        'action': action,
        'action_detail': action_detail,
        'question': dependency.get('question', ''),
        'constraint': dependency.get('constraint', 'sum_should_equal_1.0')
    }
    
    # Add market metadata if available
    if market_info:
        opportunity['market_metadata'] = {
            'liquidity': market_info.get('liquidity', 0),
            'volume': market_info.get('volume', 0),
            'volume24hr': market_info.get('volume24hr', 0),
            'slug': market_info.get('slug', ''),
            'end_day': market_info.get('end_day')
        }
    
    return opportunity


def check_combinatorial_arbitrage(
    dependency: Dict[str, Any],
    outcome_prices: Dict[str, Dict[str, float]],
    market_by_id: Dict[str, Dict],
    fee_rate: float = 0.02,
    min_profit_threshold: float = 0.0001,
    max_position_value: float = 0.95  # Per paper: ignore if any position > 95%
) -> Optional[Dict[str, Any]]:
    """
    Check if a mutually exclusive dependency has combinatorial arbitrage opportunity.
    
    Based on paper Definition 4 (Combinatorial Arbitrage):
    - If sum(subset_A) < sum(subset_B): buy subset_A, sell complement of subset_B
    - If sum(subset_A) > sum(subset_B): buy complement of subset_A, sell subset_B
    
    For simplicity, we check if sum("Yes" in market A) != sum("Yes" in market B)
    when they should be equal (mutually exclusive).
    """
    markets = dependency.get('markets', [])
    if len(markets) != 2:
        return None
    
    market_a_id, market_b_id = markets[0], markets[1]
    
    # Get prices for both markets
    prices_a = outcome_prices.get(market_a_id, {})
    prices_b = outcome_prices.get(market_b_id, {})
    
    if not prices_a or not prices_b:
        return None
    
    # Get subset outcomes to check (default to "Yes" if not specified)
    subset_a = dependency.get('subset_a', ['Yes'])
    subset_b = dependency.get('subset_b', ['Yes'])
    
    # Calculate sum of prices for each subset
    sum_a = sum(prices_a.get(outcome, 0) for outcome in subset_a if outcome in prices_a)
    sum_b = sum(prices_b.get(outcome, 0) for outcome in subset_b if outcome in prices_b)
    
    # Check if any position is too certain (per paper filtering)
    max_price_a = max(prices_a.values()) if prices_a else 0
    max_price_b = max(prices_b.values()) if prices_b else 0
    
    if max_price_a > max_position_value or max_price_b > max_position_value:
        return None  # Market too certain, skip
    
    # For mutually exclusive markets: sum_a + sum_b should be ≤ 1.0
    # If sum > 1.0: sell both (they can't both be true, but prices suggest they might)
    # If sum < 1.0: buy both (guaranteed one will be true, but prices are undervalued)
    total_sum = sum_a + sum_b
    expected_sum = 1.0
    violation = total_sum - expected_sum
    
    if abs(violation) < 0.0001:  # Too small to matter
        return None
    
    # Determine strategy based on violation
    if violation > 0:
        # Total sum > 1.0: Sell both "Yes" positions
        # Since they're mutually exclusive, at most one can be true
        # Selling both at prices that sum to >1.0 guarantees profit
        gross_profit = violation
        action = 'sell_both_yes'
        action_detail = f"Sell 'Yes' in market {market_a_id} (price={sum_a:.4f}) " \
                       f"and 'Yes' in market {market_b_id} (price={sum_b:.4f}). " \
                       f"Total={total_sum:.4f} > 1.0, but only one can be true."
    else:
        # Total sum < 1.0: Buy both "Yes" positions
        # Since they're mutually exclusive, exactly one must be true
        # Buying both at prices that sum to <1.0 guarantees profit
        gross_profit = abs(violation)
        action = 'buy_both_yes'
        action_detail = f"Buy 'Yes' in market {market_a_id} (price={sum_a:.4f}) " \
                       f"and 'Yes' in market {market_b_id} (price={sum_b:.4f}). " \
                       f"Total={total_sum:.4f} < 1.0, but one must be true."
    
    # Account for fees
    net_profit = gross_profit * (1 - fee_rate)
    
    if net_profit < min_profit_threshold:
        return None
    
    # Get market info
    market_a_info = market_by_id.get(market_a_id, {})
    market_b_info = market_by_id.get(market_b_id, {})
    
    # Determine what each outcome actually represents
    questions = dependency.get('questions', {})
    outcome_a_meaning = dependency.get('outcome_a_meaning', questions.get(market_a_id, 'Yes in Market A'))
    outcome_b_meaning = dependency.get('outcome_b_meaning', questions.get(market_b_id, 'Yes in Market B'))
    
    opportunity = {
        'market_a_id': market_a_id,
        'market_b_id': market_b_id,
        'event_id': dependency.get('event_id'),
        'type': 'combinatorial',
        'subset_a': subset_a,
        'subset_b': subset_b,
        'outcome_a_meaning': outcome_a_meaning,  # What "Yes" in market A actually means
        'outcome_b_meaning': outcome_b_meaning,  # What "Yes" in market B actually means
        'sum_a': sum_a,
        'sum_b': sum_b,
        'total_sum': total_sum,
        'expected_sum': expected_sum,
        'violation': violation,
        'violation_percent': abs(violation) * 100,
        'gross_profit': gross_profit,
        'fee_rate': fee_rate,
        'net_profit': net_profit,
        'net_profit_percent': net_profit * 100,
        'action': action,
        'action_detail': action_detail,
        'questions': questions,
        'constraint': dependency.get('constraint', 'sums_should_be_equal')
    }
    
    # Add market metadata
    if market_a_info or market_b_info:
        opportunity['market_metadata'] = {
            'market_a': {
                'liquidity': market_a_info.get('liquidity', 0),
                'volume': market_a_info.get('volume', 0),
                'question': market_a_info.get('question_clean', '')
            },
            'market_b': {
                'liquidity': market_b_info.get('liquidity', 0),
                'volume': market_b_info.get('volume', 0),
                'question': market_b_info.get('question_clean', '')
            }
        }
    
    return opportunity


def detect_arbitrage_opportunities(
    dependencies_path: str = "dependencies.json",
    outcomes_path: str = "outcomes_clean.json",
    markets_path: str = "markets_clean.json",
        fee_rate: float = 0.02,
        min_profit_threshold: float = 0.0001,  # 0.01% default (lowered to catch more)
    max_violation: float = 0.5  # Ignore violations > 50% (likely data errors)
) -> Dict[str, Any]:
    """
    Main function to detect arbitrage opportunities.
    
    Args:
        dependencies_path: Path to dependencies.json
        outcomes_path: Path to outcomes_clean.json
        markets_path: Path to markets_clean.json
        fee_rate: Trading fee rate (default 2%)
        min_profit_threshold: Minimum profit to consider (default 1%)
        max_violation: Maximum violation to consider (default 50%, filters data errors)
    
    Returns:
        Dictionary with detected arbitrage opportunities
    """
    print("=" * 60)
    print("Arbitrage Detection")
    print("=" * 60)
    
    # Load data
    dependencies, outcomes, market_by_id = load_data(
        dependencies_path, outcomes_path, markets_path
    )
    
    # Build price index
    print("\nBuilding price index...")
    outcome_prices = build_outcome_price_index(outcomes)
    print(f"Indexed prices for {len(outcome_prices)} markets")
    
    # Check complementary dependencies (Market Rebalancing Arbitrage)
    print("\nChecking complementary dependencies for arbitrage...")
    opportunities = []
    
    for dep in dependencies.get('complementary', []):
        market_id = dep['market_id']
        market_info = market_by_id.get(market_id)
        
        opportunity = check_complementary_arbitrage(
            dep,
            outcome_prices,
            market_info,
            fee_rate=fee_rate,
            min_profit_threshold=min_profit_threshold
        )
        
        if opportunity:
            # Filter out extreme violations (likely data errors)
            if abs(opportunity['violation']) <= max_violation:
                opportunities.append(opportunity)
    
    # Check mutually exclusive dependencies (Combinatorial Arbitrage)
    print("\nChecking mutually exclusive dependencies for combinatorial arbitrage...")
    combinatorial_opportunities = []
    
    for dep in dependencies.get('mutually_exclusive', []):
        opportunity = check_combinatorial_arbitrage(
            dep,
            outcome_prices,
            market_by_id,
            fee_rate=fee_rate,
            min_profit_threshold=min_profit_threshold
        )
        
        if opportunity:
            # Filter out extreme violations
            if opportunity['violation'] <= max_violation:
                combinatorial_opportunities.append(opportunity)
    
    # Combine both types
    opportunities.extend(combinatorial_opportunities)
    
    # Sort by profit (highest first)
    opportunities.sort(key=lambda x: x['net_profit'], reverse=True)
    
    # Calculate statistics
    total_opportunities = len(opportunities)
    total_gross_profit = sum(o['gross_profit'] for o in opportunities)
    total_net_profit = sum(o['net_profit'] for o in opportunities)
    avg_profit = total_net_profit / total_opportunities if total_opportunities > 0 else 0
    max_profit = opportunities[0]['net_profit'] if opportunities else 0
    
    # Group by type and action
    complementary_opps = [o for o in opportunities if o.get('type') == 'complementary']
    combinatorial_opps = [o for o in opportunities if o.get('type') == 'combinatorial']
    sell_opportunities = [o for o in opportunities if o.get('action') in ['sell_both', 'sell_complement_a_buy_subset_b']]
    buy_opportunities = [o for o in opportunities if o.get('action') in ['buy_both', 'buy_subset_a_sell_complement_b']]
    
    results = {
        'summary': {
            'total_opportunities': total_opportunities,
            'complementary_opportunities': len(complementary_opps),
            'combinatorial_opportunities': len(combinatorial_opps),
            'sell_opportunities': len(sell_opportunities),
            'buy_opportunities': len(buy_opportunities),
            'total_gross_profit': total_gross_profit,
            'total_net_profit': total_net_profit,
            'average_profit': avg_profit,
            'max_profit': max_profit,
            'fee_rate': fee_rate,
            'min_profit_threshold': min_profit_threshold
        },
        'opportunities': opportunities,
        'complementary_opportunities': complementary_opps,
        'combinatorial_opportunities': combinatorial_opps,
        'sell_opportunities': sell_opportunities,
        'buy_opportunities': buy_opportunities
    }
    
    return results


def save_opportunities(opportunities: Dict[str, Any], output_path: str = "arbitrage_opportunities.json"):
    """Save arbitrage opportunities to JSON file."""
    print(f"\nSaving opportunities to {output_path}...")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(opportunities, f, indent=2, ensure_ascii=False, default=str)
    
    summary = opportunities['summary']
    print(f"Saved {summary['total_opportunities']} arbitrage opportunities")
    print(f"  - Complementary (Market Rebalancing): {summary.get('complementary_opportunities', 0)}")
    print(f"  - Combinatorial (Cross-Market): {summary.get('combinatorial_opportunities', 0)}")
    print(f"  - Sell opportunities: {summary['sell_opportunities']}")
    print(f"  - Buy opportunities: {summary['buy_opportunities']}")
    print(f"  - Total net profit potential: {summary['total_net_profit']:.4f} ({summary['total_net_profit']*100:.2f}%)")
    print(f"  - Average profit: {summary['average_profit']:.4f} ({summary['average_profit']*100:.2f}%)")
    print(f"  - Max profit: {summary['max_profit']:.4f} ({summary['max_profit']*100:.2f}%)")


def print_top_opportunities(opportunities: Dict[str, Any], top_n: int = 10):
    """Print top N arbitrage opportunities."""
    opps = opportunities['opportunities'][:top_n]
    
    print("\n" + "=" * 60)
    print(f"Top {min(top_n, len(opps))} Arbitrage Opportunities")
    print("=" * 60)
    
    for i, opp in enumerate(opps, 1):
        opp_type = opp.get('type', 'unknown')
        
        if opp_type == 'complementary':
            # Single market arbitrage
            print(f"\n{i}. [Market Rebalancing Arbitrage] Market ID: {opp['market_id']}")
            question = opp.get('question', 'N/A')
            if len(question) > 80:
                question = question[:77] + "..."
            print(f"   Question: {question}")
            
            print(f"\n   Price Analysis:")
            outcome_a = opp['outcome_names'][0]
            outcome_b = opp['outcome_names'][1]
            price_a = opp['outcomes'][outcome_a]
            price_b = opp['outcomes'][outcome_b]
            print(f"     '{outcome_a}' price: {price_a:.4f} ({price_a*100:.2f}%)")
            print(f"     '{outcome_b}' price: {price_b:.4f} ({price_b*100:.2f}%)")
            print(f"     Total sum: {opp['price_sum']:.4f} (expected: {opp['expected_sum']:.4f})")
            print(f"     Violation: {opp['violation']:+.4f} ({opp['violation_percent']:.2f}%)")
            
            print(f"\n   Where's the Arbitrage?")
            if opp['violation'] > 0:
                print(f"     [PROBLEM] Prices sum to {opp['price_sum']:.4f} > 1.0")
                print(f"     [FACT] '{outcome_a}' and '{outcome_b}' are complementary (exactly one must be true)")
                print(f"     [SOLUTION] Sell both positions")
                print(f"        - You receive: {price_a:.4f} + {price_b:.4f} = {opp['price_sum']:.4f}")
                print(f"        - Maximum payout: 1.0 (only one can resolve true)")
                print(f"        - Guaranteed profit: {opp['price_sum']:.4f} - 1.0 = {opp['gross_profit']:.4f}")
            else:
                print(f"     [PROBLEM] Prices sum to {opp['price_sum']:.4f} < 1.0")
                print(f"     [FACT] '{outcome_a}' and '{outcome_b}' are complementary (exactly one must be true)")
                print(f"     [SOLUTION] Buy both positions")
                print(f"        - You pay: {price_a:.4f} + {price_b:.4f} = {opp['price_sum']:.4f}")
                print(f"        - Guaranteed payout: 1.0 (one will resolve true)")
                print(f"        - Guaranteed profit: 1.0 - {opp['price_sum']:.4f} = {opp['gross_profit']:.4f}")
            
            print(f"\n   Action: {opp['action_detail']}")
            print(f"   Net Profit (after {opp['fee_rate']*100:.1f}% fees): {opp['net_profit']:.4f} ({opp['net_profit_percent']:.2f}%)")
            if 'market_metadata' in opp:
                print(f"   Liquidity: ${opp['market_metadata'].get('liquidity', 0):.2f}")
        
        elif opp_type == 'combinatorial':
            # Multi-market arbitrage
            print(f"\n{i}. [Combinatorial Arbitrage] Markets: {opp['market_a_id']} & {opp['market_b_id']}")
            questions = opp.get('questions', {})
            question_a = questions.get(opp['market_a_id'], 'N/A')
            question_b = questions.get(opp['market_b_id'], 'N/A')
            if len(question_a) > 60:
                question_a = question_a[:57] + "..."
            if len(question_b) > 60:
                question_b = question_b[:57] + "..."
            print(f"   Market A: {question_a}")
            print(f"   Market B: {question_b}")
            print(f"\n   Price Analysis:")
            # Show what each outcome actually represents
            outcome_a_desc = opp.get('outcome_a_meaning', questions.get(opp['market_a_id'], 'Yes'))
            outcome_b_desc = opp.get('outcome_b_meaning', questions.get(opp['market_b_id'], 'Yes'))
            
            # Truncate long descriptions
            if len(outcome_a_desc) > 50:
                outcome_a_desc = outcome_a_desc[:47] + "..."
            if len(outcome_b_desc) > 50:
                outcome_b_desc = outcome_b_desc[:47] + "..."
            
            print(f"     Market A outcome: '{outcome_a_desc}' = {opp['sum_a']:.4f} ({opp['sum_a']*100:.2f}%)")
            print(f"     Market B outcome: '{outcome_b_desc}' = {opp['sum_b']:.4f} ({opp['sum_b']*100:.2f}%)")
            total_sum = opp.get('total_sum', opp['sum_a'] + opp['sum_b'])
            expected_sum = opp.get('expected_sum', 1.0)
            print(f"     Total sum: {total_sum:.4f} (expected: <= {expected_sum:.4f})")
            print(f"     Violation: {opp['violation']:+.4f} ({opp['violation_percent']:.2f}%)")
            
            print(f"\n   Where's the Arbitrage?")
            print(f"     [COMPARING] Two DIFFERENT outcomes:")
            print(f"       - Outcome 1: '{outcome_a_desc}' (from Market A)")
            print(f"       - Outcome 2: '{outcome_b_desc}' (from Market B)")
            print(f"     [RELATIONSHIP] These outcomes are mutually exclusive (only one can be true)")
            if opp['violation'] > 0:
                print(f"     [PROBLEM] Prices sum to {total_sum:.4f} > 1.0")
                print(f"     [FACT] These markets are mutually exclusive (only one can be true)")
                print(f"     [SOLUTION] Sell both 'Yes' positions")
                print(f"        - You receive: {opp['sum_a']:.4f} + {opp['sum_b']:.4f} = {total_sum:.4f}")
                print(f"        - Maximum payout: 1.0 (only one can resolve true)")
                print(f"        - Guaranteed profit: {total_sum:.4f} - 1.0 = {opp['gross_profit']:.4f}")
            else:
                print(f"     [PROBLEM] Prices sum to {total_sum:.4f} < 1.0")
                print(f"     [FACT] These markets are mutually exclusive (one must be true)")
                print(f"     [SOLUTION] Buy both 'Yes' positions")
                print(f"        - You pay: {opp['sum_a']:.4f} + {opp['sum_b']:.4f} = {total_sum:.4f}")
                print(f"        - Guaranteed payout: 1.0 (one will resolve true)")
                print(f"        - Guaranteed profit: 1.0 - {total_sum:.4f} = {opp['gross_profit']:.4f}")
            
            print(f"\n   Action: {opp['action_detail']}")
            print(f"   Net Profit (after {opp['fee_rate']*100:.1f}% fees): {opp['net_profit']:.4f} ({opp['net_profit_percent']:.2f}%)")
            if 'market_metadata' in opp:
                meta = opp['market_metadata']
                print(f"   Market A Liquidity: ${meta.get('market_a', {}).get('liquidity', 0):.2f}")
                print(f"   Market B Liquidity: ${meta.get('market_b', {}).get('liquidity', 0):.2f}")
        
        else:
            # Fallback for unknown types
            print(f"\n{i}. [Unknown Type] {opp.get('type', 'N/A')}")
            print(f"   Details: {str(opp)[:100]}...")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Detect arbitrage opportunities")
    parser.add_argument(
        "--dependencies",
        default="dependencies.json",
        help="Path to dependencies.json"
    )
    parser.add_argument(
        "--outcomes",
        default="outcomes_clean.json",
        help="Path to outcomes_clean.json"
    )
    parser.add_argument(
        "--markets",
        default="markets_clean.json",
        help="Path to markets_clean.json"
    )
    parser.add_argument(
        "--output",
        default="arbitrage_opportunities.json",
        help="Output path for opportunities JSON"
    )
    parser.add_argument(
        "--fee-rate",
        type=float,
        default=0.02,
        help="Trading fee rate (default: 0.02 = 2%%)"
    )
    parser.add_argument(
        "--min-profit",
        type=float,
        default=0.0001,
        help="Minimum profit threshold (default: 0.0001 = 0.01%%)"
    )
    parser.add_argument(
        "--max-violation",
        type=float,
        default=0.5,
        help="Maximum violation to consider (default: 0.5 = 50%%, filters data errors)"
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top opportunities to print (default: 10)"
    )
    
    args = parser.parse_args()
    
    opportunities = detect_arbitrage_opportunities(
        dependencies_path=args.dependencies,
        outcomes_path=args.outcomes,
        markets_path=args.markets,
        fee_rate=args.fee_rate,
        min_profit_threshold=args.min_profit,
        max_violation=args.max_violation
    )
    
    save_opportunities(opportunities, args.output)
    print_top_opportunities(opportunities, top_n=args.top_n)
    
    print("\n" + "=" * 60)
    print("Detection Complete!")
    print("=" * 60)
