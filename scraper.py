"""
Orderbook Health Monitor - GitHub Actions Scraper
This script runs every hour via GitHub Actions, scrapes orderbook data,
saves results to the repository, and sends Telegram alerts.
"""

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import time
import os
from datetime import datetime
import csv
import requests
import json

# Configuration
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Pairs to monitor
PAIRS = [
    ("USDT-NGN", 1, 0.007),
    ("BTC-USDT", 2, 0.0007),
    ("ETH-USDT", 3, 0.001),
    ("SOL-USDT", 4, 0.002),
]

MAX_RETRIES = 3


def parse_orderbook(text: str):
    """Parse orderbook text into structured dataframes."""
    def parse_number(value):
        if not value or "--" in value:
            return None
        try:
            if '{' in value and '}' in value:
                import re
                match = re.search(r"0\.0\{(\d+)\}(\d+)", value)
                if match:
                    zeros = int(match.group(1))
                    digits = match.group(2)
                    value = "0." + ("0" * zeros) + digits
            if value.endswith('K'):
                return float(value[:-1]) * 1_000
            elif value.endswith('M'):
                return float(value[:-1]) * 1_000_000
            return float(value.replace(',', ''))
        except:
            return None
    
    lines = text.split("\n")
    asks, bids = [], []
    spread_price, spread_pct = None, None
    side = "asks"
    
    for line in lines:
        if "Spread" in line:
            side = "bids"
            continue
            
        parts = line.split()
        
        if len(parts) == 1:
            try:
                spread_price = float(parts[0].replace(',', ''))
            except:
                pass
                
        elif len(parts) == 2:
            try:
                pct = parts[1].replace('(', '').replace('%)', '').replace('+', '')
                spread_pct = float(pct)
            except:
                pass
                
        elif len(parts) == 3:
            try:
                p_val = parse_number(parts[0])
                amt = parse_number(parts[1])
                tot = parse_number(parts[2])
                
                if amt is not None and tot is not None:
                    row = {"price": p_val, "amount": amt, "total": tot}
                    if side == "asks":
                        asks.append(row)
                    else:
                        bids.append(row)
            except ValueError:
                continue
    
    asks_df = pd.DataFrame(asks, columns=["price", "amount", "total"])
    bids_df = pd.DataFrame(bids, columns=["price", "amount", "total"])
    
    if not asks_df.empty:
        asks_df = asks_df.sort_values("price", ascending=False).reset_index(drop=True)
    if not bids_df.empty:
        bids_df = bids_df.sort_values("price", ascending=False).reset_index(drop=True)
        
    spread_df = pd.DataFrame([{"spread_price": spread_price, "spread_percent": spread_pct}])
    
    return asks_df, bids_df, spread_df


def calculate_liquidity_depth(asks_df, bids_df, spread_pct):
    """Calculate total liquidity depth within spread_pct of mid-price."""
    if asks_df.empty or bids_df.empty:
        return None
    
    best_ask = asks_df['price'].min()
    best_bid = bids_df['price'].max()
    mid_price = (best_ask + best_bid) / 2
    
    upper_bound = mid_price * (1 + spread_pct / 100)
    lower_bound = mid_price * (1 - spread_pct / 100)
    
    valid_bids = bids_df[bids_df['price'] >= lower_bound].copy()
    valid_asks = asks_df[asks_df['price'] <= upper_bound].copy()
    
    bid_depth = 0
    ask_depth = 0
    
    if not valid_bids.empty:
        valid_bids['quote_value'] = valid_bids['price'] * valid_bids['amount']
        bid_depth = valid_bids['quote_value'].sum()
    
    if not valid_asks.empty:
        valid_asks['quote_value'] = valid_asks['price'] * valid_asks['amount']
        ask_depth = valid_asks['quote_value'].sum()
    
    return bid_depth + ask_depth


def calculate_dws(asks_df, bids_df, num_levels=10):
    """Calculate Dollar-Weighted Spread."""
    if asks_df.empty or bids_df.empty:
        return None
    
    best_ask = asks_df['price'].min()
    best_bid = bids_df['price'].max()
    mid_price = (best_ask + best_bid) / 2
    
    asks_subset = asks_df.nsmallest(num_levels, 'price')
    bids_subset = bids_df.nlargest(num_levels, 'price')
    
    total_size = 0
    weighted_sum = 0
    
    for _, row in asks_subset.iterrows():
        size = row['amount']
        price = row['price']
        total_size += size
        weighted_sum += size * abs(price - mid_price)
    
    for _, row in bids_subset.iterrows():
        size = row['amount']
        price = row['price']
        total_size += size
        weighted_sum += size * abs(mid_price - price)
    
    if total_size == 0:
        return None
    
    return weighted_sum / total_size


def format_number(value):
    """Format large numbers with K/M suffixes."""
    if value is None:
        return "--"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    elif value >= 1_000:
        return f"{value / 1_000:.2f}K"
    else:
        return f"{value:.2f}"


def send_telegram_message(message):
    """Send message via Telegram bot."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram not configured, skipping message")
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        'chat_id': TELEGRAM_CHAT_ID,
        'text': message,
        'parse_mode': 'HTML'
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"Telegram send error: {e}")
        return False


def init_driver():
    """Initialize Chrome WebDriver for GitHub Actions."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    # GitHub Actions uses chromium-chromedriver
    chrome_options.binary_location = "/usr/bin/chromium-browser"
    
    driver = webdriver.Chrome(options=chrome_options)
    return driver


def scrape_market(driver, symbol, tab_index, target_spread):
    """Scrape a single market and return results."""
    print(f"Scraping {symbol}...")
    
    try:
        # Click market tab
        tab_selector = f'#root > div.Layout_container__jZNgw > div.Layout_inner__KhE-L > div.Layout_content__3xB4v > div > div.trade_container__nL_kd > div.trade_left__RsrvR > div.market_container__yxVbz > div > div:nth-child({tab_index})'
        WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, tab_selector))
        ).click()
        
        time.sleep(2)
        
        # Get orderbook data
        orderbook_selector = '#root > div.Layout_container__jZNgw > div.Layout_inner__KhE-L > div.Layout_content__3xB4v > div > div.trade_container__nL_kd > div.trade_right__hL85e > div > div > div:nth-child(1) > div.tabs_container__KpAcZ > div.tabs_tab_panel__ScNXg.tabs_active__Tr0AY > div > div'
        
        element = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, orderbook_selector))
        )
        
        text = element.text
        
        if "Spread" not in text:
            raise ValueError("Spread data not found in orderbook")
        
        # Parse orderbook
        asks_df, bids_df, spread_df = parse_orderbook(text)
        
        current_spread = spread_df['spread_percent'].iloc[0]
        
        # Calculate metrics
        dws_value = calculate_dws(asks_df, bids_df)
        depth_1pct = calculate_liquidity_depth(asks_df, bids_df, 1.0)
        depth_2pct = calculate_liquidity_depth(asks_df, bids_df, 2.0)
        
        # Health check
        percent_diff = ((current_spread - target_spread) / target_spread) * 100
        is_healthy = abs(percent_diff) <= 50
        
        result = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'symbol': symbol,
            'target_spread': target_spread,
            'current_spread': round(current_spread, 6),
            'percent_diff': round(percent_diff, 2),
            'dws': round(dws_value, 6) if dws_value else None,
            'depth_1pct': round(depth_1pct, 2) if depth_1pct else None,
            'depth_2pct': round(depth_2pct, 2) if depth_2pct else None,
            'status': 'Healthy' if is_healthy else 'Warning',
            'depth_1pct_display': format_number(depth_1pct),
            'depth_2pct_display': format_number(depth_2pct)
        }
        
        print(f"✓ {symbol}: Spread={current_spread:.4f}, Status={result['status']}")
        
        return result
    
    except Exception as e:
        print(f"✗ {symbol}: Failed - {str(e)}")
        return {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'symbol': symbol,
            'target_spread': target_spread,
            'current_spread': None,
            'percent_diff': None,
            'dws': None,
            'depth_1pct': None,
            'depth_2pct': None,
            'status': 'Failed',
            'depth_1pct_display': '--',
            'depth_2pct_display': '--',
            'error': str(e)[:100]
        }


def check_previous_status(symbol):
    """Check previous status from historical data."""
    try:
        df = pd.read_csv('data/latest.csv')
        prev = df[df['symbol'] == symbol]
        if not prev.empty:
            return prev.iloc[0]['status']
    except:
        pass
    return 'Healthy'


def send_alerts(results):
    """Send Telegram alerts for unhealthy markets."""
    warnings = [r for r in results if r['status'] == 'Warning']
    
    if not warnings:
        return
    
    # Check which warnings are new
    new_warnings = []
    for result in warnings:
        prev_status = check_previous_status(result['symbol'])
        if prev_status != 'Warning':
            new_warnings.append(result)
    
    if not new_warnings:
        return
    
    # Send alert for new warnings
    for result in new_warnings:
        message = f"""
🚨 <b>ORDERBOOK HEALTH ALERT</b>

<b>Market:</b> {result['symbol']}
<b>Status:</b> ⚠️ WARNING

<b>Spread Metrics:</b>
• Current: {result['current_spread']:.6f}
• Target: {result['target_spread']}
• Deviation: {result['percent_diff']:+.2f}%

<b>Liquidity Depth:</b>
• 1% Depth: {result['depth_1pct_display']}
• 2% Depth: {result['depth_2pct_display']}
• DWS: {result['dws']:.6f if result['dws'] else '--'}

<b>Reason:</b> Spread {'too wide' if result['percent_diff'] > 0 else 'too tight'} ({abs(result['percent_diff']):.1f}% from target)
"""
        send_telegram_message(message.strip())


def save_results(results):
    """Save results to CSV files."""
    # Ensure data directory exists
    os.makedirs('data', exist_ok=True)
    
    # Save timestamped file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    df = pd.DataFrame(results)
    df.to_csv(f'data/orderbook_{timestamp}.csv', index=False)
    
    # Update latest.csv for dashboard
    df.to_csv('data/latest.csv', index=False)
    
    # Append to history log
    history_file = 'data/history.csv'
    file_exists = os.path.exists(history_file)
    
    df.to_csv(history_file, mode='a', header=not file_exists, index=False)
    
    print(f"✓ Saved results to data/orderbook_{timestamp}.csv")


def main():
    """Main scraping function."""
    print("=" * 60)
    print(f"Orderbook Health Monitor - {datetime.now()}")
    print("=" * 60)
    
    # Send start notification
    send_telegram_message(f"🔄 Starting hourly orderbook check...")
    
    driver = None
    results = []
    
    try:
        # Initialize driver
        driver = init_driver()
        driver.get("https://www.quidax.com/trade")
        time.sleep(3)
        
        # Scrape each market
        for symbol, tab_index, target_spread in PAIRS:
            result = scrape_market(driver, symbol, tab_index, target_spread)
            results.append(result)
            time.sleep(1)  # Brief pause between markets
        
        # Save results
        save_results(results)
        
        # Send alerts if needed
        send_alerts(results)
        
        # Summary
        healthy_count = sum(1 for r in results if r['status'] == 'Healthy')
        warning_count = sum(1 for r in results if r['status'] == 'Warning')
        failed_count = sum(1 for r in results if r['status'] == 'Failed')
        
        summary = f"""
✅ <b>Hourly Check Complete</b>

<b>Results:</b>
• ✅ Healthy: {healthy_count}
• ⚠️ Warnings: {warning_count}
• ❌ Failed: {failed_count}

<i>Next check in 1 hour</i>
"""
        
        if warning_count > 0 or failed_count > 0:
            send_telegram_message(summary.strip())
        
        print("\n" + "=" * 60)
        print(f"Summary: {healthy_count} healthy, {warning_count} warnings, {failed_count} failed")
        print("=" * 60)
        
    except Exception as e:
        print(f"Critical error: {e}")
        send_telegram_message(f"❌ Scraping failed: {str(e)}")
        raise
    
    finally:
        if driver:
            driver.quit()


if __name__ == '__main__':
    main()
