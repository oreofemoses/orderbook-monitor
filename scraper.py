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
import json
import requests
from datetime import datetime

# --- Configuration ---
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

DATA_DIR = "data"
STATE_FILE = os.path.join(DATA_DIR, "health_state.json")
BASE_URL = "https://pro.quidax.io/en_US/trade/"

PAIRS = [
    ['AAVE_USDT', 0.30], ['ADA_USDT', 0.26], ['ALGO_USDT', 2.00],
    ['BCH_USDT', 0.26], ['BNB_USDT', 0.30], ['BONK_USDT', 2.00],
    ['BTC_USDT', 0.20], ['CAKE_USDT', 0.30], ['CFX_USDT', 2.00],['DASH_USDT', 2.00],
    ['DOT_USDT', 0.26], ['DOGE_USDT', 0.26], ['ETH_USDT', 0.25],
    ['FARTCOIN_USDT', 2.00], ['FLOKI_USDT', 0.50], ['HYPE_USDT', 2.00],
    ['LINK_USDT', 0.26],['LSK_USDT', 1.50], ['LTC_USDT', 0.30], ['NEAR_USDT', 2.00], ['NOS_USDT', 2.00],
    ['PEPE_USDT', 0.50], ['POL_USDT', 0.50], ['QDX_USDT', 10.00],
    ['RENDER_USDT', 2.00], ['Sonic_USDT', 2.00], ['SHIB_USDT', 0.40],
    ['SLP_USDT', 2.00], ['SOL_USDT', 0.25], ['STRK_USDT', 2.00],
    ['SUI_USDT', 2.00], ['TON_USDT', 0.30], ['TRX_USDT', 0.30],
    ['USDC_USDT', 0.02], ['WIF_USDT', 2.00], ['XLM_USDT', 0.30],
    ['XRP_USDT', 0.30], ['XYO_USDT', 1.00], ['ZKSync_USDT', 2.00],
    ['BTC_NGN', 0.50], ['USDT_NGN', 0.52], ['QDX_NGN', 10.00],
    ['ETH_NGN', 0.50], ['TRX_NGN', 0.50], ['XRP_NGN', 0.50],
    ['DASH_NGN', 0.50], ['LTC_NGN', 0.50], ['SOL_NGN', 0.50],
    ['USDC_NGN', 0.50]
]

# --- Utilities ---

def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", 
                     json={'chat_id': TELEGRAM_CHAT_ID, 'text': msg, 'parse_mode': 'HTML'}, timeout=10)
    except Exception as e:
        print(f"Telegram failed: {e}")

def parse_number(value):
    if not value or "--" in str(value): return None
    try:
        val_str = str(value).replace(',', '').strip()
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
    asks, bids, side, spread_pct = [], [], "asks", None
    for line in lines:
        if "Spread" in line:
            side = "bids"
            continue
        parts = line.split()
        if len(parts) == 2 and "(" in parts[1]:
            try: spread_pct = float(parts[1].replace('(', '').replace('%)', '').replace('+', ''))
            except: pass
        elif len(parts) == 3:
            p, a, t = parse_number(parts[0]), parse_number(parts[1]), parse_number(parts[2])
            if a is not None:
                row = {"price": p, "amount": a, "total": t}
                if side == "asks": asks.append(row)
                else: bids.append(row)
    return pd.DataFrame(asks), pd.DataFrame(bids), spread_pct

def calculate_liquidity_depth(asks_df, bids_df, range_multiplier):
    if asks_df.empty or bids_df.empty: return 0
    mid = (asks_df['price'].min() + bids_df['price'].max()) / 2
    upper, lower = mid * (1 + range_multiplier/100), mid * (1 - range_multiplier/100)
    return bids_df[bids_df['price'] >= lower]['total'].sum() + asks_df[asks_df['price'] <= upper]['total'].sum()

def calculate_dws(asks_df, bids_df, levels=10):
    if asks_df.empty or bids_df.empty: return 0
    mid = (asks_df['price'].min() + bids_df['price'].max()) / 2
    a_sub, b_sub = asks_df.nsmallest(levels, 'price'), bids_df.nlargest(levels, 'price')
    num = (a_sub['amount'] * (a_sub['price'] - mid)).abs().sum() + (b_sub['amount'] * (mid - b_sub['price'])).abs().sum()
    den = a_sub['amount'].sum() + b_sub['amount'].sum()
    return (num / den) / mid * 100 if den > 0 else 0

def init_chrome_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    chrome_options.add_argument(f"user-agent={user_agent}")
    if os.path.exists("/usr/bin/chromium-browser"): chrome_options.binary_location = "/usr/bin/chromium-browser"
    service = Service("/usr/bin/chromedriver")
    try: return webdriver.Chrome(service=service, options=chrome_options)
    except: return webdriver.Chrome(options=chrome_options)

# --- Main Logic ---

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # 1. PING AT START
    start_time = datetime.now()
    send_telegram(f"🔄 <b>Starting Hourly Orderbook Check</b>\nPairs to scan: {len(PAIRS)}\nTime: {start_time.strftime('%H:%M:%S')}")

    state = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f: state = json.load(f)
    
    driver = init_chrome_driver()
    results = []
    
    try:
        for symbol, target in PAIRS:
            print(f"🔍 Scrapping {symbol}...")
            try:
                driver.get(f"{BASE_URL}{symbol}")
                wait = WebDriverWait(driver, 12)
                selector = ".newTrade-depth-block.depath-index-container"
                element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                wait.until(lambda d: "Spread" in element.text and any(c.isdigit() for c in element.text))
                
                asks_df, bids_df, curr_spread = parse_orderbook(element.text)
                
                if curr_spread is not None:
                    diff_pct = ((curr_spread - target) / target) * 100
                    is_poor = (diff_pct > 50 or diff_pct < -40) # 50% threshold
                    
                    # Update State
                    p_state = state.get(symbol, {"consecutive": 0})
                    if is_poor: p_state["consecutive"] += 1
                    else: p_state["consecutive"] = 0
                    state[symbol] = p_state
                    
                    # Store Metrics
                    results.append({
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'symbol': symbol,
                        'current_spread': curr_spread,
                        'target_spread': target,
                        'percent_diff': round(diff_pct, 2),
                        'status': 'Warning' if is_poor else 'Healthy',
                        'dws': round(calculate_dws(asks_df, bids_df), 4),
                        'depth_1pct_display': f"${calculate_liquidity_depth(asks_df, bids_df, curr_spread*1.25)/1000:.1f}K",
                        'strikes': p_state["consecutive"]
                    })
            except Exception:
                print(f"❌ Skipped {symbol}")

        # 2. PING AT END (Summary Report)
        warnings = [r for r in results if r['status'] == 'Warning']
        end_time = datetime.now()
        
        report = f"✅ <b>Check Complete</b> ({end_time.strftime('%H:%M:%S')})\n"
        report += f"Total: {len(results)} | Healthy: {len(results)-len(warnings)} | ⚠️ Warning: {len(warnings)}\n"
        
        if warnings:
            report += "\n📊 <b>WARNING DETAILS:</b>\n"
            # Sort warnings by worst deviation
            warnings = sorted(warnings, key=lambda x: x['percent_diff'], reverse=True)
            
            for w in warnings:
                report += (
                    f"----------------------------\n"
                    f"<b>{w['symbol']}</b> (Strike: {w['strikes']})\n"
                    f"• Spread: <code>{w['current_spread']}%</code> (Target: {w['target_spread']}%)\n"
                    f"• Diff: <code>{w['percent_diff']:+.1f}%</code>\n"
                    f"• DWS: <code>{w['dws']:.4f}%</code>\n"
                    f"• Depth: <code>{w['depth_1pct_display']}</code>\n"
                )
        
        # If the report is too long for one Telegram message (max 4096 chars), 
        # it might need splitting, but with 50 pairs and only warnings, this format should fit.
        send_telegram(report)

        # Save Files
        df = pd.DataFrame(results)
        df.to_csv(f"{DATA_DIR}/latest.csv", index=False)
        with open(STATE_FILE, 'w') as f: json.dump(state, f)
        
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
