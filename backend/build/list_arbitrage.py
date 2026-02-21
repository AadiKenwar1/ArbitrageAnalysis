"""
List Arbitrage Opportunities

Displays arbitrage opportunities in a readable format.
"""

import json
import sys
from typing import Dict, List, Any


def load_opportunities(filepath: str = "arbitrage_opportunities.json") -> Dict[str, Any]:
    """Load arbitrage opportunities from JSON file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERROR: {filepath} not found.")
        print("Run the pipeline first: python run_pipeline.py")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"ERROR: {filepath} is not valid JSON.")
        sys.exit(1)


def format_opportunity(opp: Dict[str, Any], index: int) -> str:
    """Format a single opportunity for display."""
    lines = []
    lines.append(f"\n{'='*80}")
    lines.append(f"Opportunity #{index}")
    lines.append(f"{'='*80}")
    
    opp_type = opp.get('type', 'unknown')
    
    # Basic info - handle both complementary and combinatorial
    if opp_type == 'complementary':
        lines.append(f"Type: Market Rebalancing Arbitrage (Single Market)")
        lines.append(f"Market ID: {opp['market_id']}")
        if opp.get('event_id'):
            lines.append(f"Event ID: {opp['event_id']}")
        
        # Question
        question = opp.get('question', 'N/A')
        if len(question) > 100:
            question = question[:97] + "..."
        lines.append(f"Question: {question}")
        
        # Outcomes and prices
        lines.append(f"\nOutcomes:")
        outcome_names = opp['outcome_names']
        outcomes = opp['outcomes']
        for name in outcome_names:
            price = outcomes[name]
            lines.append(f"  - {name}: {price:.6f} ({price*100:.4f}%)")
        
        # Price analysis
        lines.append(f"\nPrice Analysis:")
        lines.append(f"  Sum: {opp['price_sum']:.6f}")
        lines.append(f"  Expected: {opp['expected_sum']:.6f}")
        lines.append(f"  Violation: {opp['violation']:+.6f} ({opp['violation_percent']:.4f}%)")
        
        # Market metadata
        if 'market_metadata' in opp:
            meta = opp['market_metadata']
            lines.append(f"\nMarket Info:")
            if meta.get('liquidity'):
                try:
                    liq = float(meta['liquidity'])
                    lines.append(f"  Liquidity: ${liq:,.2f}")
                except (ValueError, TypeError):
                    lines.append(f"  Liquidity: {meta['liquidity']}")
            if meta.get('volume'):
                try:
                    vol = float(meta['volume'])
                    lines.append(f"  Volume: ${vol:,.2f}")
                except (ValueError, TypeError):
                    lines.append(f"  Volume: {meta['volume']}")
            if meta.get('volume24hr'):
                try:
                    vol24 = float(meta['volume24hr'])
                    lines.append(f"  24hr Volume: ${vol24:,.2f}")
                except (ValueError, TypeError):
                    lines.append(f"  24hr Volume: {meta['volume24hr']}")
            if meta.get('end_day'):
                lines.append(f"  End Date: {meta['end_day']}")
    
    elif opp_type == 'combinatorial':
        lines.append(f"Type: Combinatorial Arbitrage (Cross-Market)")
        lines.append(f"Market A ID: {opp['market_a_id']}")
        lines.append(f"Market B ID: {opp['market_b_id']}")
        if opp.get('event_id'):
            lines.append(f"Event ID: {opp['event_id']}")
        
        # Questions
        questions = opp.get('questions', {})
        question_a = questions.get(opp['market_a_id'], 'N/A')
        question_b = questions.get(opp['market_b_id'], 'N/A')
        if len(question_a) > 100:
            question_a = question_a[:97] + "..."
        if len(question_b) > 100:
            question_b = question_b[:97] + "..."
        lines.append(f"Market A: {question_a}")
        lines.append(f"Market B: {question_b}")
        
        # Outcomes being compared
        outcome_a_desc = opp.get('outcome_a_meaning', 'Yes in Market A')
        outcome_b_desc = opp.get('outcome_b_meaning', 'Yes in Market B')
        if len(outcome_a_desc) > 80:
            outcome_a_desc = outcome_a_desc[:77] + "..."
        if len(outcome_b_desc) > 80:
            outcome_b_desc = outcome_b_desc[:77] + "..."
        
        lines.append(f"\nOutcomes Being Compared:")
        lines.append(f"  Market A: '{outcome_a_desc}' = {opp['sum_a']:.6f} ({opp['sum_a']*100:.4f}%)")
        lines.append(f"  Market B: '{outcome_b_desc}' = {opp['sum_b']:.6f} ({opp['sum_b']*100:.4f}%)")
        
        # Price analysis
        total_sum = opp.get('total_sum', opp['sum_a'] + opp['sum_b'])
        expected_sum = opp.get('expected_sum', 1.0)
        lines.append(f"\nPrice Analysis:")
        lines.append(f"  Total Sum: {total_sum:.6f}")
        lines.append(f"  Expected: <= {expected_sum:.6f}")
        lines.append(f"  Violation: {opp['violation']:+.6f} ({opp['violation_percent']:.4f}%)")
        
        # Market metadata
        if 'market_metadata' in opp:
            meta = opp['market_metadata']
            lines.append(f"\nMarket Info:")
            if 'market_a' in meta:
                ma = meta['market_a']
                if ma.get('liquidity'):
                    try:
                        lines.append(f"  Market A Liquidity: ${float(ma['liquidity']):,.2f}")
                    except (ValueError, TypeError):
                        pass
            if 'market_b' in meta:
                mb = meta['market_b']
                if mb.get('liquidity'):
                    try:
                        lines.append(f"  Market B Liquidity: ${float(mb['liquidity']):,.2f}")
                    except (ValueError, TypeError):
                        pass
    
    else:
        # Fallback for unknown types
        lines.append(f"Type: {opp_type}")
        if 'market_id' in opp:
            lines.append(f"Market ID: {opp['market_id']}")
        if 'market_a_id' in opp:
            lines.append(f"Market A ID: {opp['market_a_id']}")
        if 'market_b_id' in opp:
            lines.append(f"Market B ID: {opp['market_b_id']}")
    
    # Profit analysis (common to all types)
    lines.append(f"\nProfit Analysis:")
    lines.append(f"  Gross Profit: {opp['gross_profit']:.6f} ({opp['gross_profit']*100:.4f}%)")
    lines.append(f"  Fee Rate: {opp['fee_rate']*100:.2f}%")
    lines.append(f"  Net Profit: {opp['net_profit']:.6f} ({opp['net_profit_percent']:.4f}%)")
    
    # Action
    lines.append(f"\nAction Required:")
    lines.append(f"  {opp['action_detail']}")
    lines.append(f"  Type: {opp['action']}")
    
    return "\n".join(lines)


def filter_realistic_opportunities(
    opportunities: List[Dict[str, Any]],
    min_net_profit_pct: float = 5.0,  # Minimum 5% net profit (high quality)
    min_liquidity: float = 2000.0,  # Minimum $2000 liquidity (executable)
    min_violation_pct: float = 2.0,  # Minimum 2% violation (significant)
    max_position_value: float = 0.95  # Skip markets that are too certain
) -> List[Dict[str, Any]]:
    """Filter opportunities to only show realistic, executable ones."""
    filtered = []
    
    for opp in opportunities:
        # Filter by minimum net profit
        if opp.get('net_profit_percent', 0) < min_net_profit_pct:
            continue
        
        # Filter by minimum violation (avoid rounding errors)
        if abs(opp.get('violation_percent', 0)) < min_violation_pct:
            continue
        
        # Filter by liquidity
        liquidity_ok = False
        if opp.get('type') == 'complementary':
            meta = opp.get('market_metadata', {})
            liq = meta.get('liquidity', 0)
            try:
                if float(liq) >= min_liquidity:
                    liquidity_ok = True
            except (ValueError, TypeError):
                pass
        elif opp.get('type') == 'combinatorial':
            meta = opp.get('market_metadata', {})
            liq_a = meta.get('market_a', {}).get('liquidity', 0)
            liq_b = meta.get('market_b', {}).get('liquidity', 0)
            try:
                if float(liq_a) >= min_liquidity and float(liq_b) >= min_liquidity:
                    liquidity_ok = True
            except (ValueError, TypeError):
                pass
        
        if not liquidity_ok:
            continue
        
        # Filter by max position value (skip markets that are too certain)
        if opp.get('type') == 'complementary':
            outcomes = opp.get('outcomes', {})
            if outcomes:
                max_price = max(outcomes.values())
                if max_price > max_position_value:
                    continue
        elif opp.get('type') == 'combinatorial':
            # For combinatorial, check if either market is too certain
            sum_a = opp.get('sum_a', 0)
            sum_b = opp.get('sum_b', 0)
            if sum_a > max_position_value or sum_b > max_position_value:
                continue
        
        filtered.append(opp)
    
    return filtered


def list_all_opportunities(
    filepath: str = "arbitrage_opportunities.json",
    limit: int = 20,  # Default to top 20
    sort_by: str = "profit",
    min_net_profit_pct: float = 5.0,
    min_liquidity: float = 2000.0,
    min_violation_pct: float = 2.0
):
    """List all arbitrage opportunities."""
    opps_data = load_opportunities(filepath)
    
    opportunities = opps_data.get('opportunities', [])
    summary = opps_data.get('summary', {})
    
    if not opportunities:
        print("No arbitrage opportunities found.")
        return
    
    # Filter to only realistic opportunities
    print(f"Filtering {len(opportunities)} opportunities...")
    opportunities = filter_realistic_opportunities(
        opportunities,
        min_net_profit_pct=min_net_profit_pct,
        min_liquidity=min_liquidity,
        min_violation_pct=min_violation_pct
    )
    print(f"Found {len(opportunities)} realistic opportunities after filtering.")
    
    # Sort if needed
    if sort_by == "profit":
        opportunities = sorted(opportunities, key=lambda x: x['net_profit'], reverse=True)
    elif sort_by == "violation":
        opportunities = sorted(opportunities, key=lambda x: abs(x['violation']), reverse=True)
    
    # Limit if specified (or use default)
    if limit is not None:
        opportunities = opportunities[:limit]
    
    # Print summary
    print("="*80)
    print("ARBITRAGE OPPORTUNITIES SUMMARY (FILTERED)")
    print("="*80)
    print(f"Total Opportunities (before filtering): {summary.get('total_opportunities', 0)}")
    print(f"Realistic Opportunities (after filtering): {len(opportunities)}")
    print(f"  - Min Net Profit: {min_net_profit_pct}%")
    print(f"  - Min Liquidity: ${min_liquidity:,.0f}")
    print(f"  - Min Violation: {min_violation_pct}%")
    if opportunities:
        sell_count = sum(1 for o in opportunities if o.get('action', '').startswith('sell'))
        buy_count = sum(1 for o in opportunities if o.get('action', '').startswith('buy'))
        print(f"  - Sell Opportunities: {sell_count}")
        print(f"  - Buy Opportunities: {buy_count}")
        total_profit = sum(o['net_profit'] for o in opportunities)
        avg_profit = total_profit / len(opportunities) if opportunities else 0
        max_profit = max(o['net_profit'] for o in opportunities) if opportunities else 0
        print(f"  - Average Profit: {avg_profit*100:.4f}%")
        print(f"  - Max Profit: {max_profit*100:.4f}%")
    print("="*80)
    
    # Print each opportunity
    for i, opp in enumerate(opportunities, 1):
        print(format_opportunity(opp, i))
    
    print(f"\n{'='*80}")
    print(f"Displayed {len(opportunities)} of {len(opportunities)} realistic opportunities")
    print(f"{'='*80}")


def list_summary_table(filepath: str = "arbitrage_opportunities.json"):
    """List opportunities in a compact table format."""
    opps_data = load_opportunities(filepath)
    opportunities = opps_data.get('opportunities', [])
    
    if not opportunities:
        print("No arbitrage opportunities found.")
        return
    
    print("\n" + "="*120)
    print("ARBITRAGE OPPORTUNITIES - TABLE VIEW")
    print("="*120)
    print(f"{'#':<4} {'Market ID':<12} {'Action':<10} {'Violation %':<12} {'Net Profit %':<14} {'Liquidity':<12} {'Question':<50}")
    print("-"*120)
    
    for i, opp in enumerate(opportunities, 1):
        market_id = opp['market_id']
        action = opp['action']
        violation_pct = f"{opp['violation_percent']:+.4f}"
        profit_pct = f"{opp['net_profit_percent']:.4f}"
        liquidity_raw = opp.get('market_metadata', {}).get('liquidity', 0)
        try:
            liquidity = float(liquidity_raw) if liquidity_raw else 0
        except (ValueError, TypeError):
            liquidity = 0
        question = opp.get('question', '')[:47] + "..." if len(opp.get('question', '')) > 50 else opp.get('question', '')
        
        print(f"{i:<4} {market_id:<12} {action:<10} {violation_pct:<12} {profit_pct:<14} ${liquidity:<11,.0f} {question:<50}")
    
    print("="*120)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="List arbitrage opportunities")
    parser.add_argument(
        "--file",
        default="arbitrage_opportunities.json",
        help="Path to arbitrage opportunities JSON file"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Limit number of opportunities to display (default: 20)"
    )
    parser.add_argument(
        "--no-limit",
        action="store_true",
        help="Show all opportunities (overrides --limit)"
    )
    parser.add_argument(
        "--sort",
        choices=["profit", "violation"],
        default="profit",
        help="Sort by profit or violation (default: profit)"
    )
    parser.add_argument(
        "--table",
        action="store_true",
        help="Display in compact table format"
    )
    parser.add_argument(
        "--min-profit",
        type=float,
        default=5.0,
        help="Minimum net profit percentage (default: 5.0%%)"
    )
    parser.add_argument(
        "--min-liquidity",
        type=float,
        default=2000.0,
        help="Minimum liquidity in USD (default: $2000)"
    )
    parser.add_argument(
        "--min-violation",
        type=float,
        default=2.0,
        help="Minimum violation percentage (default: 2.0%%)"
    )
    
    args = parser.parse_args()
    
    if args.table:
        list_summary_table(args.file)
    else:
        limit_value = None if args.no_limit else args.limit
        list_all_opportunities(
            args.file,
            limit=limit_value,
            sort_by=args.sort,
            min_net_profit_pct=args.min_profit,
            min_liquidity=args.min_liquidity,
            min_violation_pct=args.min_violation
        )
