# Tests for Polymarket Trader

This directory contains unit tests for the `PolymarketTrader` class and its methods.

## Test Files

- `test_polymarket_trader_init.py` - Tests for the `__init__` method
- `test_get_token_id.py` - Tests for the `get_token_id` method
- `test_execute_trade.py` - Tests for the `execute_trade` method

## Running Tests

To run all tests:
```bash
python -m pytest tests/
```

Or using unittest:
```bash
python -m unittest discover tests
```

To run a specific test file:
```bash
python -m pytest tests/test_get_token_id.py
```

To run a specific test case:
```bash
python -m pytest tests/test_get_token_id.py::TestGetTokenId::test_get_token_id_first_market_yes
```

## Test Data

The tests use `test.json` from the parent directory as sample event data. This file contains a real Polymarket event structure with markets, outcomes, and token IDs.

## Mocking

The tests use `unittest.mock` to:
- Mock API calls to avoid making real HTTP requests
- Mock the ClobClient to avoid needing real credentials
- Isolate the code under test

## Test Coverage

The tests cover:
- Successful token ID retrieval for different outcomes (YES/NO)
- Token ID retrieval with and without sub_slug
- Case-insensitive outcome matching
- Error handling for invalid sub_slug, invalid outcome, and missing markets
- Trade execution with different sides (BUY/SELL), prices, sizes, and order types
- Proper initialization of the ClobClient and API credentials

