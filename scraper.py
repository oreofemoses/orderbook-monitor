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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# --- Load Pairs Configuration ---
def load_pairs_config():
    pairs_json = os.getenv('PAIRS_CONFIG')
    if not pairs_json:
        print("❌ ERROR: PAIRS_CONFIG environment variable not set!")
        exit(1)
    try:
        print("Loading pairs from environment variable...")
        pairs = json.loads(pairs_json)
        print(f"✅ Loaded {len(pairs)} pairs")
        return pairs
    except json.JSONDecodeError as e:
        print(f"❌ ERROR: Invalid JSON in PAIRS_CONFIG: {e}")
        exit(1)

# --- Configuration ---
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
chat_ids_str = os.getenv('TELEGRAM_CHAT_ID')
TELEGRAM_CHAT_IDS = chat_ids_str.split(',') if chat_ids_str else []

NIGERIAN_TZ = timezone(timedelta(hours=1))

DATA_DIR = "data"
STATE_FILE = os.path.join(DATA_DIR, "health_state.json")
BASE_URL = "https://pro.quidax.io/en_US/trade/"

ALERT_THRESHOLD_CYCLES = 3
ALERT_COOLDOWN_MINUTES = 30
MAX_ATTEMPTS_PER_PAIR = 3
MIN_ORDERBOOK_LAYERS = 10
MID_PRICE_ALERT_THRESHOLD = 25

# ── Parallelism ──────────────────────────────────────────────────────────────
# Safe ceiling for GitHub Actions (2-core runner, ~7GB RAM).
# Each Chrome instance uses ~300–400MB. 5 workers ≈ 2GB peak — well within limits.
# Raise to 8 if running on a larger self-hosted runner.
MAX_WORKERS = 5

# ── Locks for shared mutable state ──────────────────────────────────────────
_state_lock     = threading.Lock()   # guards the health state dict
_results_lock   = threading.Lock()   # guards final_results list
_spikes_lock    = threading.Lock()   # guards spike_summary list
_telegram_lock  = threading.Lock()   # serialises Telegram sends (avoids 429s)

CURRENCY_SYMBOLS = {
    "USDT": "$",
    "NGN":  "₦",
    "GHS":  "₵",
}

HIGH_VOL_TOKENS = {'BTC', 'ETH', 'SOL', 'USDC'}

def get_threshold(sym):
    base = sym.split('_')[0].upper()
    if sym == 'USDT_NGN':
        return 50_000_000
    if sym.endswith('_NGN'):
        return 50_000_000 if base in HIGH_VOL_TOKENS else 5_000_000
    if sym.endswith('_GHS') or sym == 'USDT_GHS':
        return 60_000
    if sym == 'CNGN_USDT':
        return None
    return 100_000 if base in HIGH_VOL_TOKENS else 5_000

def get_currency_symbol(sym):
    quote = sym.split("_")[-1].upper()
    return CURRENCY_SYMBOLS.get(quote, "$")

PAIRS = load_pairs_config()

def get_nigerian_time():
    return datetime.now(NIGERIAN_TZ)

# --- Core Computation Functions ---

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
    except:
        return None

def parse_orderbook(text: str):
    lines = text.split("\n")
    asks, bids, side, spread_pct, mid_price = [], [], "asks", None, None
    for line in lines:
        if "Spread" in line:
            side = "bids"
            continue
        parts = line.split()
        if len(parts) == 1:
            try:
                mid_price = float(parts[0].replace(',', ''))
            except:
                pass
        elif len(parts) == 2 and "(" in parts[1]:
            try:
                spread_pct = float(parts[1].replace('(', '').replace('%)', '').replace('+', ''))
            except:
                pass
        elif len(parts) == 3:
            p, a, t = parse_number(parts[0]), parse_number(parts[1]), parse_number(parts[2])
            if a is not None:
                row = {"price": p, "amount": a, "total": t}
                if side == "asks":
                    asks.append(row)
                else:
                    bids.append(row)
    asks_df = pd.DataFrame(asks)
    bids_df = pd.DataFrame(bids)
    return asks_df, bids_df, spread_pct, mid_price, len(asks_df), len(bids_df)

def calculate_liquidity_depth(asks_df, bids_df, spread_pct_range):
    if asks_df.empty or bids_df.empty: return 0
    mid = (asks_df['price'].min() + bids_df['price'].max()) / 2
    upper = mid * (1 + spread_pct_range / 100)
    lower = mid * (1 - spread_pct_range / 100)
    bid_depth = (bids_df[bids_df['price'] >= lower]['price'] * bids_df[bids_df['price'] >= lower]['amount']).sum()
    ask_depth = (asks_df[asks_df['price'] <= upper]['price'] * asks_df[asks_df['price'] <= upper]['amount']).sum()
    return bid_depth + ask_depth

def calculate_dws(asks_df, bids_df, num_levels=10):
    if asks_df.empty or bids_df.empty: return 0
    mid = (asks_df['price'].min() + bids_df['price'].max()) / 2
    a_sub = asks_df.nsmallest(num_levels, 'price')
    b_sub = bids_df.nlargest(num_levels, 'price')
    num = (a_sub['amount'] * (a_sub['price'] - mid)).abs().sum() + (b_sub['amount'] * (mid - b_sub['price'])).abs().sum()
    den = a_sub['amount'].sum() + b_sub['amount'].sum()
    return (num / den) / mid * 100 if den > 0 else 0

def format_depth(val):
    if not val: return "$0"
    if val >= 1_000_000: return f"${val/1_000_000:.2f}M"
    if val >= 1_000: return f"${val/1_000:.1f}K"
    return f"${val:.0f}"

# --- Spike Detection ---

def get_todays_trades(raw_text, sym):
    now = datetime.now()
    current_secs = now.hour * 3600 + now.minute * 60 + now.second
    rows = []
    for line in raw_text.strip().split("\n"):
        parts = line.split()
        if len(parts) == 3 and parts[0] not in ("Price", "--"):
            try:
                price_str = parts[0].replace(",", "")
                if "{5}" in price_str:
                    price_str = price_str.replace("{5}", "00000")
                h, m, s = parts[2].split(":")
                secs = int(h) * 3600 + int(m) * 60 + int(s)
                rows.append({
                    "pair": sym,
                    "price": float(price_str),
                    "amount": float(parts[1].replace(",", "")),
                    "time": parts[2],
                    "secs": secs,
                })
            except:
                continue
    if not rows:
        return []
    newest = rows[0]["secs"]
    if newest > current_secs:
        return []
    todays = []
    prev_secs = newest
    for row in rows:
        if row["secs"] > prev_secs:
            break
        value = round(row["price"] * row["amount"], 2)
        todays.append({**row, "value": value})
        prev_secs = row["secs"]
    return todays

def get_hourly_spikes(trades, sym):
    if not trades:
        return []
    threshold = get_threshold(sym)
    currency  = get_currency_symbol(sym)
    hourly = defaultdict(list)
    for t in trades:
        hour = int(t["time"].split(":")[0])
        hourly[hour].append(t)
    spikes = []
    for hour in sorted(hourly.keys()):
        bucket    = hourly[hour]
        total_val = sum(t["value"] for t in bucket)
        if total_val >= threshold:
            spikes.append({
                "hour":        hour,
                "trade_count": len(bucket),
                "total_value": total_val,
                "currency":    currency,
            })
    return spikes

# --- Persistence & Helpers ---

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

def update_daily_log(all_results):
    nigerian_time = get_nigerian_time()
    today = nigerian_time.strftime("%Y-%m-%d")
    path = os.path.join(DATA_DIR, f"daily_log_{today}.csv")
    current_time = nigerian_time.strftime("%H:%M:%S")
    pairs_lookup = {pair[0]: pair[1] for pair in PAIRS}

    if os.path.exists(path):
        df = pd.read_csv(path)
        existing_markets = set(df['Market'].tolist())
        missing_pairs = [(sym, tgt) for sym, tgt in PAIRS if sym not in existing_markets]
        if missing_pairs:
            existing_check_cols = [c for c in df.columns if c not in ('Market', '% Spd', 'DEPTH')]
            new_rows = []
            for sym, tgt in missing_pairs:
                row = {'Market': sym}
                for col in existing_check_cols:
                    row[col] = ''
                if 'DEPTH' in df.columns:
                    row['DEPTH'] = ''
                new_rows.append(row)
            new_rows_df = pd.DataFrame(new_rows, columns=df.columns)
            df = pd.concat([df, new_rows_df], ignore_index=True)
            print(f"ℹ️  Added {len(missing_pairs)} new pair(s) to today's log")
    else:
        df = pd.DataFrame({'Market': [pair[0] for pair in PAIRS]})

    status_cols = [col for col in df.columns if col.startswith('STATUS (CHECK')]
    check_num = len(status_cols) + 1
    status_col = f'STATUS (CHECK {check_num})'
    time_col   = f'TIME (CHECK {check_num})'
    results_map = {r['symbol']: r for r in all_results}

    df[status_col] = df['Market'].apply(
        lambda m: results_map[m]['status'].upper() if m in results_map else 'SKIPPED'
    )
    df[time_col] = df['Market'].apply(
        lambda m: current_time if m in results_map else ''
    )

    if 'DEPTH' in df.columns:
        df['DEPTH'] = df.apply(
            lambda row: f"{results_map[row['Market']]['depth_1.25x']} / {results_map[row['Market']]['depth_1.5x']}"
            if row['Market'] in results_map else row['DEPTH'],
            axis=1
        )
    else:
        df['DEPTH'] = df['Market'].apply(
            lambda m: f"{results_map[m]['depth_1.25x']} / {results_map[m]['depth_1.5x']}"
            if m in results_map else ''
        )

    cols = [col for col in df.columns if col != 'DEPTH']
    cols.append('DEPTH')
    df = df[cols]
    df.to_csv(path, index=False)
    print(f"✅ Daily log updated: {path}")

def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_IDS:
        return
    with _telegram_lock:
        for chat_id in TELEGRAM_CHAT_IDS:
            chat_id = chat_id.strip()
            if chat_id:
                try:
                    requests.post(
                        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                        json={'chat_id': chat_id, 'text': msg, 'parse_mode': 'HTML'},
                        timeout=10
                    )
                except Exception as e:
                    print(f"⚠️  Telegram send failed: {e}")

def init_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    )

    selenium_remote_url = os.getenv('SELENIUM_REMOTE_URL')
    if selenium_remote_url:
        print(f"Using remote Selenium at {selenium_remote_url}")
        driver = webdriver.Remote(command_executor=selenium_remote_url, options=chrome_options)
    else:
        print("Using local Chrome")
        if os.path.exists("/usr/bin/chromium-browser"):
            chrome_options.binary_location = "/usr/bin/chromium-browser"
        service = Service("/usr/bin/chromedriver")
        try:
            driver = webdriver.Chrome(service=service, options=chrome_options)
        except:
            driver = webdriver.Chrome(options=chrome_options)

    return driver

# ---------------------------------------------------------------------------
# Per-pair worker — runs in its own thread with its own Chrome instance
# ---------------------------------------------------------------------------

def scrape_pair(symbol, target, shared_state):
    """
    Scrapes one trading pair: orderbook + trade feed.
    Returns a result dict on success, or None on total failure.
    Writes alert Telegrams inline (thread-safe via _telegram_lock).
    Reads/writes shared_state under _state_lock.
    """
    monitor_only = (target is None)
    driver = None

    try:
        driver = init_driver()
        attempt = 0

        while attempt < MAX_ATTEMPTS_PER_PAIR:
            attempt += 1
            try:
                driver.get(f"{BASE_URL}{symbol}")
                wait = WebDriverWait(driver, 15)

                # ── Orderbook ──────────────────────────────────────────────
                ob_selector = ".newTrade-depth-block.depath-index-container"
                ob_element  = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ob_selector)))
                try:
                    wait.until(lambda d: "Spread" in ob_element.text and any(c.isdigit() for c in ob_element.text))
                except Exception:
                    pass

                asks_df, bids_df, curr_spread, mid_price, ask_layers, bid_layers = parse_orderbook(ob_element.text)
                if curr_spread is None:
                    raise ValueError("No spread data")

                # ── Spread anomaly check ───────────────────────────────────
                if not monitor_only:
                    diff = ((curr_spread - target) / target) * 100
                    spread_anomaly = (diff > 100 or diff < -75)
                else:
                    diff = None
                    spread_anomaly = False

                shallow_orderbook = (ask_layers < MIN_ORDERBOOK_LAYERS or bid_layers < MIN_ORDERBOOK_LAYERS)
                is_poor = spread_anomaly or shallow_orderbook

                # ── Calculations ───────────────────────────────────────────
                dws      = calculate_dws(asks_df, bids_df)
                depth_25 = calculate_liquidity_depth(asks_df, bids_df, curr_spread * 1.25)
                depth_50 = calculate_liquidity_depth(asks_df, bids_df, curr_spread * 1.5)

                # ── Thread-safe state read/write ───────────────────────────
                with _state_lock:
                    p_state = shared_state.get(symbol, {
                        "consecutive": 0,
                        "last_alert":  None,
                        "start_time":  None,
                        "last_mid_price": None,
                    })

                    # Mid-price change alert
                    last_mid_price = p_state.get("last_mid_price")
                    if mid_price is not None and last_mid_price is not None:
                        price_change_pct = ((mid_price - last_mid_price) / last_mid_price) * 100
                        if abs(price_change_pct) >= MID_PRICE_ALERT_THRESHOLD:
                            direction = "📈" if price_change_pct > 0 else "📉"
                            send_telegram(
                                f"{direction} <b>PRICE MOVEMENT ALERT: {symbol}</b>\n"
                                f"Previous Mid: {last_mid_price:,.6g}\n"
                                f"Current Mid:  {mid_price:,.6g}\n"
                                f"Change: {price_change_pct:+.2f}%"
                            )
                    p_state["last_mid_price"] = mid_price

                    # Strike accumulation / clearing
                    if is_poor:
                        p_state["consecutive"] += 1
                        if not p_state["start_time"]:
                            p_state["start_time"] = get_nigerian_time().isoformat()
                    else:
                        p_state["consecutive"] = 0
                        p_state["start_time"]  = None
                        p_state["last_alert"]  = None

                    shared_state[symbol] = p_state

                # ── Alert logic ────────────────────────────────────────────
                if p_state["consecutive"] >= ALERT_THRESHOLD_CYCLES:
                    last_alert  = p_state.get("last_alert")
                    cooldown_ok = True
                    if last_alert:
                        last_alert_time = datetime.fromisoformat(last_alert)
                        if last_alert_time.tzinfo is None:
                            last_alert_time = last_alert_time.replace(tzinfo=NIGERIAN_TZ)
                        if get_nigerian_time() - last_alert_time < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                            cooldown_ok = False

                    if cooldown_ok:
                        alert_msg = f"⚠️ <b>ALERT: {symbol}</b>\n"
                        if not monitor_only:
                            alert_msg += f"Spread: {curr_spread}% (Target: {target}%)\n"
                            alert_msg += f"Diff: {diff:+.2f}%\n"
                        else:
                            alert_msg += f"Spread: {curr_spread}% (Monitor only)\n"
                        alert_msg += f"Ask Layers: {ask_layers}\n"
                        alert_msg += f"Bid Layers: {bid_layers}\n"
                        alert_msg += f"Strikes: {p_state['consecutive']}\n"
                        alert_msg += f"Depth @ 1.25x: {format_depth(depth_25)}\n"
                        alert_msg += f"Depth @ 1.5x: {format_depth(depth_50)}"
                        if spread_anomaly and shallow_orderbook:
                            alert_msg += "\n🚨 BOTH spread & orderbook issues"
                        elif spread_anomaly:
                            alert_msg += "\n📊 Spread anomaly detected"
                        elif shallow_orderbook:
                            alert_msg += "\n📉 Shallow orderbook detected"
                        send_telegram(alert_msg)
                        with _state_lock:
                            shared_state[symbol]["last_alert"] = get_nigerian_time().isoformat()

                # ── Spike detection ────────────────────────────────────────
                pair_spikes = []
                try:
                    trade_selector = ".currentTrade.currentTrade-index-container"
                    trade_element  = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, trade_selector)))
                    wait.until(lambda d: "Price" in trade_element.text and any(c.isdigit() for c in trade_element.text))
                    time.sleep(0.5)

                    trades      = get_todays_trades(trade_element.text, symbol)
                    pair_spikes = get_hourly_spikes(trades, symbol)

                    if pair_spikes:
                        print(f"[{symbol}] 🚨 {len(pair_spikes)} spiked hour(s) detected")
                    else:
                        print(f"[{symbol}] ✅ No trade spikes")
                except Exception as spike_err:
                    print(f"[{symbol}] Spike check skipped: {spike_err}")

                # ── Build result ───────────────────────────────────────────
                result = {
                    'timestamp':      get_nigerian_time().strftime('%Y-%m-%d %H:%M:%S'),
                    'symbol':         symbol,
                    'monitor_only':   monitor_only,
                    'status':         'Warning' if is_poor else 'Checked',
                    'strikes':        p_state["consecutive"],
                    'current_spread': curr_spread,
                    'target_spread':  target if not monitor_only else 'N/A',
                    'percent_diff':   round(diff, 2) if diff is not None else 'N/A',
                    'ask_layers':     ask_layers,
                    'bid_layers':     bid_layers,
                    'dws':            round(dws, 4),
                    'depth_1.25x':    format_depth(depth_25),
                    'depth_1.5x':     format_depth(depth_50),
                    '_spikes':        pair_spikes,   # internal — stripped before CSV
                }
                print(f"[{symbol}] ✓ done (attempt {attempt})")
                return result

            except Exception as e:
                print(f"[{symbol}] attempt {attempt} failed: {e}")
                if attempt < MAX_ATTEMPTS_PER_PAIR:
                    time.sleep(2)

        print(f"[{symbol}] ✗ all {MAX_ATTEMPTS_PER_PAIR} attempts failed")
        return None

    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

# --- Main Execution ---

def main():
    os.makedirs(DATA_DIR, exist_ok=True)

    # Load state once — workers read/write via shared reference under _state_lock
    shared_state = load_state()

    final_results = []
    spike_summary = []

    run_start = get_nigerian_time()
    send_telegram(f"🔄 <b>Starting Hourly Check</b>\nPairs: {len(PAIRS)}\nWorkers: {MAX_WORKERS}")

    # ── Parallel scrape ──────────────────────────────────────────────────────
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all pairs at once; executor caps concurrency at MAX_WORKERS
        future_to_symbol = {
            executor.submit(scrape_pair, symbol, target, shared_state): symbol
            for symbol, target in PAIRS
        }

        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                result = future.result()
                if result is not None:
                    # Extract spike data before storing result
                    pair_spikes = result.pop('_spikes', [])

                    with _results_lock:
                        final_results.append(result)

                    if pair_spikes:
                        with _spikes_lock:
                            spike_summary.append({"symbol": symbol, "spikes": pair_spikes})
            except Exception as exc:
                print(f"[{symbol}] unhandled exception in future: {exc}")

    run_end     = get_nigerian_time()
    elapsed_sec = (run_end - run_start).total_seconds()
    print(f"\n⏱  Parallel scrape complete in {elapsed_sec:.0f}s for {len(final_results)}/{len(PAIRS)} pairs")

    # ── Persist results ──────────────────────────────────────────────────────
    update_daily_log(final_results)

    if final_results:
        # Sort back into PAIRS order for consistent CSV output
        pair_order = {sym: i for i, (sym, _) in enumerate(PAIRS)}
        final_results.sort(key=lambda r: pair_order.get(r['symbol'], 9999))

        new_df = pd.DataFrame(final_results)
        new_df.to_csv(os.path.join(DATA_DIR, "latest.csv"), index=False)
        print("✅ Data saved to latest.csv")

    # Save state — all worker writes are done by this point
    save_state(shared_state)

    # ── Spike summary Telegram ───────────────────────────────────────────────
    if spike_summary:
        spike_msg  = "🚨 <b>Trade Spike Summary</b>\n"
        spike_msg += f"<i>{run_end.strftime('%Y-%m-%d %H:%M:%S')} (NGN)</i>\n"
        spike_msg += f"{'─' * 30}\n"
        for entry in spike_summary:
            spike_msg += f"\n<b>{entry['symbol']}</b>\n"
            for s in entry["spikes"]:
                spike_msg += (
                    f"  {s['hour']:02d}:00 — "
                    f"{s['trade_count']} trade(s) — "
                    f"{s['currency']}{s['total_value']:,.2f}\n"
                )
        spike_msg += f"\n{len(spike_summary)} pair(s) flagged"
        send_telegram(spike_msg)
    else:
        send_telegram("✅ <b>No trade spikes detected this check.</b>")

    # ── Completion message ───────────────────────────────────────────────────
    warnings    = [r for r in final_results if r['status'] == 'Warning']
    total_pairs = len(final_results)

    completion_msg  = f"✅ <b>Check Complete</b>\n"
    completion_msg += f"Total Pairs: {total_pairs} / {len(PAIRS)}\n"
    completion_msg += f"Warnings: {len(warnings)}\n"
    completion_msg += f"Duration: {elapsed_sec:.0f}s\n"

    if warnings:
        completion_msg += "\n<b>⚠️ Markets with Warnings:</b>\n"
        for w in warnings:
            completion_msg += f"\n<b>{w['symbol']}</b>"
            if not w['monitor_only']:
                completion_msg += f"\n  Spread: {w['current_spread']}% (Target: {w['target_spread']}%)"
                completion_msg += f"\n  Diff: {w['percent_diff']:+.2f}%"
            else:
                completion_msg += f"\n  Spread: {w['current_spread']}% (Monitor only)"
            completion_msg += f"\n  Ask Layers: {w['ask_layers']}"
            completion_msg += f"\n  Bid Layers: {w['bid_layers']}"
            completion_msg += f"\n  Strikes: {w['strikes']}"
            completion_msg += f"\n  DWS: {w['dws']}"
            completion_msg += f"\n  Depth @ 1.25x: {w['depth_1.25x']}"
            completion_msg += f"\n  Depth @ 1.5x: {w['depth_1.5x']}\n"

    send_telegram(completion_msg)

if __name__ == "__main__":
    main()
