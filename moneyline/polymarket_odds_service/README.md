# Polymarket Odds Service

This folder provides a service to fetch and analyze Polymarket market data, including orderbook statistics for prediction markets.

## Features

- Fetches event data from Polymarket's Gamma API
- Extracts token IDs from event markets
- Fetches orderbooks from Polymarket's CLOB API
- Calculates market statistics (best bid/ask, volumes, spreads)

## Usage

### Interface (Recommended)

```python
from polymarket_odds_service.polymarket_odds_interface import PolymarketOddsInterface

interface = PolymarketOddsInterface("Chicago Bulls", "Detroit Pistons", date(2026, 1, 7))
results = interface.get_market_odds()

for market in results:
    print(f"{market.market}: Bid={market.best_bid}, Ask={market.best_ask}")
```

### Return Format

Each result dictionary contains:
- `market`: Market question with outcome label (e.g., "Bulls vs. Pistons (Bulls)")
- `best_bid`: Highest bid price (float)
- `bid_volume`: Total bid volume (float)
- `best_ask`: Lowest ask price (float)
- `ask_volume`: Total ask volume (float)
- `spread`: Spread (best_ask - best_bid) (float)

### Direct Use

You can also use `PolymarketMarketAnalyzer` directly:

```python
from polymarket_odds_service.polymarket_market_analyzer import PolymarketMarketAnalyzer

analyzer = PolymarketMarketAnalyzer()
results = analyzer.analyze_markets("event-slug")
analyzer.display_results(results)  # Prints formatted table
```

## Files

- `polymarket_odds_interface.py` - Main interface for fetching market odds
- `polymarket_market_analyzer.py` - Standalone market analyzer class
- `fetch_market_data.py` - Script with example usage
- `find_nba_game.py` - Utility to find NBA games on Polymarket

## Dependencies

- `requests` - For API calls
- `tabulate` - For formatted table output (optional, only for display_results)

