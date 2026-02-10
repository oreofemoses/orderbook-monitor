"""
Orderbook Health Monitor - GitHub Actions Scraper
Corrected version for reliability in headless environments.
"""

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import time
import os
import re
from datetime import datetime
import requests

# --- Configuration ---
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Pairs to monitor: (Symbol, Tab Index in UI, Target Spread %)
PAIRS = [
    ("USDT-NGN", 1, 0.007),
    ("BTC-USDT", 2, 0.0007),
    ("ETH-USDT", 3, 0.001),
    ("SOL-USDT", 4, 0.002),
]

def parse_number(value):
    """Robust parsing for crypto values (handles 0.0{6}123, 1.2K, 5M, commas)."""
    if not value or "--" in str(value):
        return None
    try:
        val_str = str(value).replace(',', '').strip()
        
        # Handle Quidax scientific notation: 0.0{6}123
        if '{' in val_str:
            match = re.search(r"0\.0\{(\d+)\}(\d+)", val_str)
            if match:
                zeros = int(match.group(1))
                digits = match.group(2)
                val_str = "0." + ("0" * zeros) + digits
        
        # Handle K and M suffixes
        if val_str.upper().endswith('K'):
            return float(val_str[:-1]) * 1_000
        elif val_str.upper().endswith('M'):
            return float(val_str[:-1]) * 1_000_000
        
        return float(val_str)
    except:
        return None

def parse_orderbook(text: str):
    """Parse raw UI text into structured data."""
    lines = text.split("\n")
    asks, bids = [], []
    spread_price, spread_pct = None, None
    side = "asks"
    
    for line in lines:
        if "Spread" in line:
            side = "bids"
            continue
            
        parts = line.split()
        
        # Capture the mid-spread price and percentage
        if len(parts) == 1 and not asks:
            try: spread_price = parse_number(parts[0])
            except: pass
        elif len(parts) == 2 and "(" in parts[1]:
            try:
                pct = parts[1].replace('(', '').replace('%)', '').replace('+', '')
                spread_pct = float(pct)
            except: pass
                
        # Capture order lines (Price, Amount, Total)
        elif len(parts) == 3:
            p_val = parse_number(parts[0])
            amt = parse_number(parts[1])
            tot = parse_number(parts[2])
            
            if p_val is not None:
                row = {"price": p_val, "amount": amt, "total": tot}
                if side == "asks":
                    asks.append(row)
                else:
                    bids.append(row)
    
    asks_df = pd.DataFrame(asks)
    bids_df = pd.DataFrame(bids)
    spread_df = pd.DataFrame([{"spread_price": spread_price, "spread_percent": spread_pct}])
    
    return asks_df, bids_df, spread_df

def calculate_liquidity_depth(asks_df, bids_df, range_pct):
    """Calculate total depth within X% of mid-price."""
    if asks_df.empty or bids_df.empty: return None
    
    mid = (asks_df['price'].min() + bids_df['price'].max()) / 2
    upper = mid * (1 + range_pct / 100)
    lower = mid * (1 - range_pct / 100)
    
    ask_depth = asks_df[asks_df['price'] <= upper]['total'].max() if not asks_df[asks_df['price'] <= upper].empty else 0
    bid_depth = bids_df[bids_df['price'] >= lower]['total'].max() if not bids_df[bids_df['price'] >= lower].empty else 0
    
    return ask_depth + bid_depth

def calculate_dws(asks_df, bids_df, levels=10):
    """Dollar-Weighted Spread."""
    if asks_df.empty or bids_df.empty: return None
    mid = (asks_df['price'].min() + bids_df['price'].max()) / 2
    
    # Take top levels
    a_sub = asks_df.nsmallest(levels, 'price')
    b_sub = bids_df.nlargest(levels, 'price')
    
    total_vol = a_sub['amount'].sum() + b_sub['amount'].sum()
    if total_vol == 0: return None
    
    weighted_sum = sum(a_sub['amount'] * (a_sub['price'] - mid)) + sum(b_sub['amount'] * (mid - b_sub['price']))
    return weighted_sum / total_vol

def send_telegram_message(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try: requests.post(url, json={'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'HTML'}, timeout=10)
    except: print("Failed to send Telegram message")

def init_driver():
    """Optimized for GitHub Actions Linux environment."""
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    # Path for chromium-browser in GitHub Actions
    binary_path = "/usr/bin/chromium-browser"
    if os.path.exists(binary_path):
        chrome_options.binary_location = binary_path
    
    return webdriver.Chrome(options=chrome_options)

def scrape_market(driver, symbol, tab_index, target_spread):
    print(f"🔍 Scraping {symbol}...")
    try:
        # Click Market Tab
        selector = f'div.market_container__yxVbz div:nth-child({tab_index})'
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector))).click()
        time.sleep(3) # Wait for orderbook to refresh
        
        # Scrape Orderbook Panel
        ob_selector = 'div.tabs_tab_panel__ScNXg.tabs_active__Tr0AY'
        element = WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, ob_selector)))
        
        asks_df, bids_df, spread_df = parse_orderbook(element.text)
        
        curr_spread = spread_df['spread_percent'].iloc[0]
        if curr_spread is None: raise ValueError("Could not extract spread")
        
        # Calculations
        diff = ((curr_spread - target_spread) / target_spread) * 100
        d1 = calculate_liquidity_depth(asks_df, bids_df, 1.0)
        d2 = calculate_liquidity_depth(asks_df, bids_df, 2.0)
        dws = calculate_dws(asks_df, bids_df)
        
        status = 'Warning' if diff > 50 else 'Healthy'
        
        print(f"✅ {symbol}: Spread {curr_spread}% (Target {target_spread}%) - {status}")
        
        return {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'symbol': symbol,
            'target_spread': target_spread,
            'current_spread': curr_spread,
            'percent_diff': round(diff, 2),
            'dws': dws,
            'depth_1pct': d1,
            'depth_2pct': d2,
            'status': status,
            'depth_1pct_display': f"{d1/1000:.1f}K" if d1 and d1 > 1000 else str(d1),
            'depth_2pct_display': f"{d2/1000:.1f}K" if d2 and d2 > 1000 else str(d2)
        }
    except Exception as e:
        print(f"❌ Error scraping {symbol}: {e}")
        return {'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'symbol': symbol, 'status': 'Failed', 'target_spread': target_spread}

def main():
    os.makedirs('data', exist_ok=True)
    driver = init_driver()
    results = []
    
    try:
        driver.get("https://www.quidax.com/trade")
        time.sleep(5)
        
        for sym, idx, target in PAIRS:
            res = scrape_market(driver, sym, idx, target)
            results.append(res)
        
        # Save results
        df = pd.DataFrame(results)
        ts = datetime.now().strftime('%Y%m%d_%H%M')
        df.to_csv(f'data/orderbook_{ts}.csv', index=False)
        df.to_csv('data/latest.csv', index=False)
        
        # Telegram Alerts
        warnings = [r for r in results if r['status'] == 'Warning']
        if warnings:
            msg = "🚨 <b>Orderbook Alerts</b>\n\n"
            for w in warnings:
                msg += f"• {w['symbol']}: Spread {w['current_spread']}% (Target {w['target_spread']}%)\n"
            send_telegram_message(msg)
            
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
