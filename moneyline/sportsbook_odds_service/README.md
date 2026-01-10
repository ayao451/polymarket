# Sportsbook Odds Service

This folder provides a service that connects to The Odds API to fetch odds from multiple sportsbooks for NBA games.

Currently supports:
- Moneyline odds

Future support planned for:
- Spreads
- Totals (Over/Under)
- Player props
- And more

## Setup
- Set `API_KEY` in your environment (or a `.env` file in this folder):

```bash
export API_KEY="YOUR_THE_ODDS_API_KEY"
```

## Install

```bash
python3 -m pip install -r requirements.txt
```

## Usage

### Main Entry Point (Recommended)

Use `main.py` in the parent directory for the most robust experience with comprehensive error handling:

```bash
python3 main.py "Chicago Bulls" "Detroit Pistons" 2026-01-07
```

Output format:
```
================================================================================
ODDS
================================================================================
Chicago Bulls @ Detroit Pistons | Chicago Bulls: 0.234678 to win $1 | Detroit Pistons: 0.801473 to win $1
================================================================================
```

The main script includes:
- Environment validation (checks for API_KEY)
- Team validation
- Comprehensive error handling at each step
- Clear error messages with troubleshooting hints

### Programmatic Use

```python
from sportsbook_odds_service.sportsbook_weighted_odds_interface import SportsbookWeightedOddsInterface
from datetime import date

interface = SportsbookWeightedOddsInterface()
result = interface.get_moneyline_odds("Chicago Bulls", "Detroit Pistons", date(2026, 1, 7))
if result:
    print(result)
else:
    print("Failed to fetch odds")
```

### Legacy CLI (Team Names)

```bash
python3 fetch_game_odds.py --team-a "Chicago Bulls" --team-b "Detroit Pistons" --show-weights
```

## Configure sportsbook weights
Edit `SPORTSBOOK_WEIGHTS` in `weighted_average.py`. Pinnacle is set to `0.50` by default; any other present books share the remaining weight equally.
