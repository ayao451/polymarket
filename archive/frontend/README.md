# Polymarket Trading Interface

A simple React-based trading interface for Polymarket.

## Features

- Enter a Polymarket market slug
- Display the market slug with Buy and Sell buttons
- Input fields for quantity and price
- Clean, simple UI with TailwindCSS
- Console logging of trade details

## Setup

1. Install dependencies:
```bash
npm install
```

## Backend (required for Execute Trade)

This UI’s **Execute Trade** button calls a local Python API which wraps `execute_trade.py`.

1. Install backend deps (from repo root):

```bash
pip install -r requirements.txt
```

2. Start the API server (from repo root):

```bash
uvicorn api_server:app --reload --port 8000
```

2. Run the development server:
```bash
npm run dev
```

3. Open your browser and navigate to the URL shown in the terminal (typically http://localhost:5173)

## Building for Production

To create a production build:
```bash
npm run build
```

To preview the production build:
```bash
npm run preview
```

## Usage

1. Enter a market slug in the "Market Slug" input field
2. The market slug will be displayed with Buy and Sell buttons
3. Enter the quantity and price for your trade
4. Click either the "Buy" or "Sell" button
5. Check the browser console to see the logged trade details

## Trade Details Format

When you click Buy or Sell, the following information is logged to the console:

```javascript
{
  action: 'BUY' or 'SELL',
  marketSlug: 'your-market-slug',
  quantity: <number>,
  price: <number>
}
```

