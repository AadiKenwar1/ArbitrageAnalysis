# Polymarket Dataset

This dataset contains all active events and markets from Polymarket, fetched via their public REST API.

## Dataset Structure

The dataset is stored as a JSON file (`polymarket_dataset.json`) with the following structure:

```json
{
  "events": [...],
  "markets": [...],
  "total_events": 7811,
  "total_markets": 30797
}
```

## Events

An **event** is a top-level object representing a question (e.g., "Who will win the 2024 Presidential Election?"). Each event object contains:

- `id`: Unique event identifier
- `slug`: URL-friendly identifier (e.g., "event-slug-name")
- `title`: Event question/title
- `description`: Detailed description of the event
- `image`: Image URL for the event
- `active`: Boolean indicating if the event is currently active
- `closed`: Boolean indicating if the event is closed
- `startDate`: ISO timestamp for event start
- `endDate`: ISO timestamp for event end
- `volume`: Total trading volume
- `volume24hr`: 24-hour trading volume
- `liquidity`: Current liquidity
- `markets`: Array of associated market objects (nested within event)
- `tags`: Array of tag objects for categorization
- `series`: Series/grouping information (if applicable)

## Markets

A **market** is a specific tradable binary outcome within an event. Each market object contains:

- `id`: Unique market identifier
- `slug`: URL-friendly identifier
- `question`: The market question
- `conditionId`: Condition identifier
- `questionId`: Question identifier
- `marketMakerAddress`: Market maker contract address
- `outcomes`: JSON string array of possible outcomes (e.g., `["Yes", "No"]`)
- `outcomePrices`: JSON string array of implied probabilities matching outcomes (e.g., `["0.20", "0.80"]`)
- `clobTokenIds`: Object mapping outcomes to CLOB token addresses for trading
- `enableOrderBook`: Boolean indicating if the market can be traded via CLOB
- `active`: Boolean indicating if the market is currently active
- `closed`: Boolean indicating if the market is closed
- `volume`: Total trading volume
- `volume24hr`: 24-hour trading volume
- `liquidity`: Current liquidity
- `event`: Reference to parent event (contains event id, slug, title)

### Important Notes on Markets

- `outcomes` and `outcomePrices` are stored as **JSON strings** and need to be parsed
- Prices represent implied probabilities (should sum to ~1.0 across all outcomes)
- The arrays map 1:1: index 0 of `outcomes` corresponds to index 0 of `outcomePrices`
- Markets can be single-outcome (Yes/No) or multi-outcome (e.g., multiple candidates)

## Dataset Statistics

- **Total Events**: 7,811 active events
- **Total Markets**: 30,797 active markets
- **File Size**: ~200+ MB (varies based on current Polymarket activity)

## Usage Example

```python
import json

with open('polymarket_dataset.json', 'r') as f:
    dataset = json.load(f)

# Access events
events = dataset['events']
print(f"Total events: {dataset['total_events']}")

# Access markets
markets = dataset['markets']
print(f"Total markets: {dataset['total_markets']}")

# Parse market outcomes and prices
for market in markets[:5]:
    outcomes = json.loads(market['outcomes'])
    prices = json.loads(market['outcomePrices'])
    print(f"{market['question']}: {dict(zip(outcomes, prices))}")
```

## Generating the Dataset

Run the ingestor script to fetch fresh data:

```bash
python backend/ingestor/ingestor.py
```

This will:
1. Fetch all active events from Polymarket API
2. Fetch all active markets from Polymarket API
3. Combine them into a single dataset
4. Save to `polymarket_dataset.json`

**Note**: The fetch process may take several minutes depending on the number of active markets.

## Data Source

Data is fetched from Polymarket's public REST API:
- **Base URL**: `https://gamma-api.polymarket.com`
- **Endpoints**: `/events` and `/markets`
- **Authentication**: None required (public API)

## Use Cases

This dataset is useful for:
- Arbitrage opportunity detection
- Market analysis and research
- Price trend analysis
- Market liquidity analysis
- Event categorization and filtering
