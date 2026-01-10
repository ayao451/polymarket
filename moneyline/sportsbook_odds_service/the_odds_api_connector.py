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


