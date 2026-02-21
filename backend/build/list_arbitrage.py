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
    
    # Basic info
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
    
    # Profit analysis
    lines.append(f"\nProfit Analysis:")
    lines.append(f"  Gross Profit: {opp['gross_profit']:.6f} ({opp['gross_profit']*100:.4f}%)")
    lines.append(f"  Fee Rate: {opp['fee_rate']*100:.2f}%")
    lines.append(f"  Net Profit: {opp['net_profit']:.6f} ({opp['net_profit_percent']:.4f}%)")
    
    # Action
    lines.append(f"\nAction Required:")
    lines.append(f"  {opp['action_detail']}")
    lines.append(f"  Type: {opp['action']}")
    
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
    
    return "\n".join(lines)


def list_all_opportunities(
    filepath: str = "arbitrage_opportunities.json",
    limit: int = None,
    sort_by: str = "profit"
):
    """List all arbitrage opportunities."""
    opps_data = load_opportunities(filepath)
    
    opportunities = opps_data.get('opportunities', [])
    summary = opps_data.get('summary', {})
    
    if not opportunities:
        print("No arbitrage opportunities found.")
        return
    
    # Sort if needed
    if sort_by == "profit":
        opportunities = sorted(opportunities, key=lambda x: x['net_profit'], reverse=True)
    elif sort_by == "violation":
        opportunities = sorted(opportunities, key=lambda x: abs(x['violation']), reverse=True)
    
    # Limit if specified
    if limit:
        opportunities = opportunities[:limit]
    
    # Print summary
    print("="*80)
    print("ARBITRAGE OPPORTUNITIES SUMMARY")
    print("="*80)
    print(f"Total Opportunities: {summary.get('total_opportunities', 0)}")
    print(f"  - Sell Opportunities: {summary.get('sell_opportunities', 0)}")
    print(f"  - Buy Opportunities: {summary.get('buy_opportunities', 0)}")
    print(f"Total Net Profit Potential: {summary.get('total_net_profit', 0)*100:.4f}%")
    print(f"Average Profit: {summary.get('average_profit', 0)*100:.4f}%")
    print(f"Max Profit: {summary.get('max_profit', 0)*100:.4f}%")
    print("="*80)
    
    # Print each opportunity
    for i, opp in enumerate(opportunities, 1):
        print(format_opportunity(opp, i))
    
    print(f"\n{'='*80}")
    print(f"Displayed {len(opportunities)} of {summary.get('total_opportunities', 0)} opportunities")
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
        default=None,
        help="Limit number of opportunities to display"
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
    
    args = parser.parse_args()
    
    if args.table:
        list_summary_table(args.file)
    else:
        list_all_opportunities(args.file, limit=args.limit, sort_by=args.sort)
