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

# Trading pairs configuration from your snippet
PAIRS = [
    ['AAVE_USDT', 0.30], ['ADA_USDT', 0.26], ['ALGO_USDT', 2.00],
    # ['BCH_USDT', 0.26], ['BNB_USDT', 0.30], ['BONK_USDT', 2.00],
    # ['BTC_USDT', 0.20], ['CAKE_USDT', 0.30], ['CFX_USDT', 2.00],['DASH_USDT', 2.00],
    # ['DOT_USDT', 0.26], ['DOGE_USDT', 0.26], ['ETH_USDT', 0.25],
    # ['FARTCOIN_USDT', 2.00], ['FLOKI_USDT', 0.50], ['HYPE_USDT', 2.00],
    # ['LINK_USDT', 0.26],['LSK_USDT', 1.50], ['LTC_USDT', 0.30], ['NEAR_USDT', 2.00], ['NOS_USDT', 2.00],
    # ['PEPE_USDT', 0.50], ['POL_USDT', 0.50], ['QDX_USDT', 10.00],
    # ['RENDER_USDT', 2.00], ['Sonic_USDT', 2.00], ['SHIB_USDT', 0.40],
    # ['SLP_USDT', 2.00], ['SOL_USDT', 0.25], ['STRK_USDT', 2.00],
    # ['SUI_USDT', 2.00], ['TON_USDT', 0.30], ['TRX_USDT', 0.30],
    # ['USDC_USDT', 0.02], ['WIF_USDT', 2.00], ['XLM_USDT', 0.30],
    # ['XRP_USDT', 0.30], ['XYO_USDT', 1.00], ['ZKSync_USDT', 2.00],
    # ['BTC_NGN', 0.50], ['USDT_NGN', 0.52], ['QDX_NGN', 10.00],
    # ['ETH_NGN', 0.50], ['TRX_NGN', 0.50], ['XRP_NGN', 0.50],
    # ['DASH_NGN', 0.50], ['LTC_NGN', 0.50], ['SOL_NGN', 0.50],
    # ['USDC_NGN', 0.50]
]

def init_chrome_driver():
    """Your provided optimized Driver configuration."""
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    chrome_options.add_argument(f"user-agent={user_agent}")
    
    if os.path.exists("/usr/bin/chromium-browser"):
        chrome_options.binary_location = "/usr/bin/chromium-browser"
    elif os.path.exists("/usr/bin/chromium"):
        chrome_options.binary_location = "/usr/bin/chromium"
    
    try:
        service = Service("/usr/bin/chromedriver")
        driver = webdriver.Chrome(service=service, options=chrome_options)
    except Exception:
        driver = webdriver.Chrome(options=chrome_options)
    
    return driver

def parse_number(value):
    if not value or "--" in str(value): return None
    try:
        val_str = str(value).replace(',', '').strip()
        # Handle Quidax scientific notation: 0.0{6}123
        if '{' in val_str:
            match = re.search(r"0\.0\{(\d+)\}(\d+)", val_str)
            if match:
                val_str = "0." + ("0" * int(match.group(1))) + match.group(2)
        if val_str.upper().endswith('K'): return float(val_str[:-1]) * 1_000
        if val_str.upper().endswith('M'): return float(val_str[:-1]) * 1_000_000
        return float(val_str)
    except: return None

def parse_orderbook(text: str):
    lines = text.split("\n")
    asks, bids = [], []
    spread_price, spread_pct = None, None
    side = "asks"
    
    for line in lines:
        if "Spread" in line:
            side = "bids"
            continue
        parts = line.split()
        if len(parts) == 1 and not asks:
            try: spread_price = parse_number(parts[0])
            except: pass
        elif len(parts) == 2 and "(" in parts[1]:
            try: spread_pct = float(parts[1].replace('(', '').replace('%)', '').replace('+', ''))
            except: pass
        elif len(parts) == 3:
            p_val = parse_number(parts[0])
            amt = parse_number(parts[1])
            tot = parse_number(parts[2])
            if p_val is not None:
                row = {"price": p_val, "amount": amt, "total": tot}
                if side == "asks": asks.append(row)
                else: bids.append(row)
    
    return pd.DataFrame(asks), pd.DataFrame(bids), spread_pct

def scrape_market(driver, symbol, target_spread):
    url = f"https://pro.quidax.io/en_US/trade/{symbol}"
    print(f"🔍 Checking {symbol}...")
    
    try:
        driver.get(url)
        # Wait for the orderbook panel to contain the word "Spread"
        wait = WebDriverWait(driver, 15)
        ob_selector = 'div.tabs_tab_panel__ScNXg.tabs_active__Tr0AY'
        element = wait.until(EC.text_to_be_present_in_element((By.CSS_SELECTOR, ob_selector), "Spread"))
        
        # Get the actual element text
        ob_text = driver.find_element(By.CSS_SELECTOR, ob_selector).text
        asks_df, bids_df, curr_spread = parse_orderbook(ob_text)
        
        if curr_spread is None:
            raise ValueError("Spread percentage not found in text")

        diff = ((curr_spread - target_spread) / target_spread) * 100
        status = 'Warning' if diff > 50 else 'Healthy'
        
        # Calculate Depth (Total cumulative value at bottom of asks/bids)
        d1 = asks_df['total'].max() if not asks_df.empty else 0
        d2 = bids_df['total'].max() if not bids_df.empty else 0
        total_depth = d1 + d2

        return {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'symbol': symbol,
            'target_spread': target_spread,
            'current_spread': curr_spread,
            'percent_diff': round(diff, 2),
            'status': status,
            'depth_1pct_display': f"{total_depth/1000:.1f}K" if total_depth > 1000 else f"{total_depth:.2f}",
            'depth_2pct_display': "N/A", # Placeholder
            'dws': 0.0 # Placeholder
        }
    except Exception as e:
        print(f"⚠️ {symbol} skipped: {e}")
        return None

def main():
    os.makedirs('data', exist_ok=True)
    driver = init_chrome_driver()
    all_results = []
    
    try:
        for pair_data in PAIRS:
            symbol = pair_data[0]
            target = pair_data[1]
            
            result = scrape_market(driver, symbol, target)
            if result:
                all_results.append(result)
            
            # Short sleep to prevent rate limiting
            time.sleep(1)

        if all_results:
            df = pd.DataFrame(all_results)
            # Save latest for dashboard
            df.to_csv('data/latest.csv', index=False)
            # Save historical
            ts = datetime.now().strftime('%Y%m%d_%H%M')
            df.to_csv(f'data/orderbook_{ts}.csv', index=False)
            
            # Telegram Alerts
            warnings = [r for r in all_results if r['status'] == 'Warning']
            if warnings:
                alert_text = f"🚨 <b>Orderbook Health Alert ({len(warnings)} issues)</b>\n"
                for w in warnings[:10]: # Limit to 10 pairs in one message
                    alert_text += f"\n• {w['symbol']}: {w['current_spread']}% (Target: {w['target_spread']}%)"
                
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", 
                             json={'chat_id': TELEGRAM_CHAT_ID, 'text': alert_text, 'parse_mode': 'HTML'})

    finally:
        driver.quit()
        print("Scrape cycle complete.")

if __name__ == "__main__":
    main()
