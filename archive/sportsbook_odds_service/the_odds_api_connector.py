import os
import requests

from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
REGIONS = "us,eu"
ODDS_FORMAT = "decimal"
DATE_FORMAT = "iso"


class theOddsAPIConnector:
    def get_odds(self, SPORT, MARKET):
        odds_response = requests.get(
            f"https://api.the-odds-api.com/v4/sports/{SPORT}/odds",
            params={
                "api_key": API_KEY,
                "regions": REGIONS,
                "markets": MARKET,
                "oddsFormat": ODDS_FORMAT,
                "dateFormat": DATE_FORMAT,
            },
        )

        if odds_response.status_code != 200:
            raise Exception(
                f"Failed to get odds: status_code {odds_response.status_code}, response body {odds_response.text}"
            )
        else:
            odds_json = odds_response.json()
            return odds_json

    def get_event_markets(self, SPORT, event_id, *, regions: str = REGIONS, date_format: str = DATE_FORMAT):
        """
        GET event markets:
        Returns available market keys for each bookmaker for a single event.

        Endpoint:
          GET /v4/sports/{sport}/events/{eventId}/markets
        """
        markets_response = requests.get(
            f"https://api.the-odds-api.com/v4/sports/{SPORT}/events/{event_id}/markets",
            params={
                "apiKey": API_KEY,  # note: this endpoint uses apiKey (camelCase) in docs
                "regions": regions,
                "dateFormat": date_format,
            },
        )

        if markets_response.status_code != 200:
            raise Exception(
                f"Failed to get event markets: status_code {markets_response.status_code}, "
                f"response body {markets_response.text}"
            )

        return markets_response.json()


