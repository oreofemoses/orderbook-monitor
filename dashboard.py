import streamlit as st
import pandas as pd
import glob
import json
import os
from datetime import datetime

st.set_page_config(
    page_title="Orderbook Health Monitor",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Orderbook Health Monitor")
st.markdown("*Real-time market liquidity and spread tracking*")

# --- Data Loading Functions ---
def load_latest_data():
    if not os.path.exists('data/latest.csv'):
        return None, None
    
    df = pd.read_csv('data/latest.csv')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Try to load consecutive strikes from state file
    strikes = {}
    if os.path.exists('data/health_state.json'):
        with open('data/health_state.json', 'r') as f:
            state = json.load(f)
            strikes = {k: v['consecutive'] for k, v in state.items()}
    
    df['strikes'] = df['symbol'].map(strikes).fillna(0).astype(int)
    return df, strikes

# --- Layout ---
latest_df, strikes_map = load_latest_data()

if latest_df is None:
    st.warning("⚠️ No data available yet. Waiting for GitHub Actions to run...")
    st.stop()

# --- Top Metrics ---
last_update = latest_df['timestamp'].max()
time_diff = datetime.now() - last_update
minutes_ago = int(time_diff.total_seconds() / 60)

m1, m2, m3, m4 = st.columns(4)
m1.metric("Last Update", last_update.strftime('%H:%M:%S'))
m2.metric("Healthy Markets", (latest_df['status'] == 'Healthy').sum())
m3.metric("Warnings", (latest_df['status'] == 'Warning').sum(), delta_color="inverse")
m4.metric("Monitor Status", "Live" if minutes_ago < 90 else "Stale", delta=f"{minutes_ago}m ago")

st.markdown("---")

# --- Main Status Table ---
st.subheader("🎯 Current Market Status")

# We only select columns that actually exist in the NEW scraper
display_cols = ['symbol', 'status', 'strikes', 'current_spread', 'target_spread', 'percent_diff', 'dws', 'depth_1pct_display']
available_cols = [c for c in display_cols if c in latest_df.columns]

display_df = latest_df[available_cols].copy()

# Rename for UI
display_df.columns = [c.replace('_', ' ').title() for c in display_df.columns]

def style_status(row):
    if row['Status'] == 'Warning':
        return ['background-color: #ffebee'] * len(row)
    return ['background-color: #e8f5e9'] * len(row)

st.dataframe(
    display_df.style.apply(style_status, axis=1),
    use_container_width=True,
    hide_index=True
)

# --- Historical Charts ---
st.markdown("---")
st.subheader("📈 24h Spread Trends")

try:
    all_files = sorted(glob.glob('data/orderbook_*.csv'))
    if len(all_files) > 1:
        # Load last 24 runs
        recent_files = all_files[-24:]
        hist_df = pd.concat([pd.read_csv(f) for f in recent_files])
        hist_df['timestamp'] = pd.to_datetime(hist_df['timestamp'])

        selected_market = st.selectbox("Select Market to Analyze", options=sorted(hist_df['symbol'].unique()))
        
        market_data = hist_df[hist_df['symbol'] == selected_market].sort_values('timestamp')
        
        # # Plotting
        # chart_data = market_data.set_index('timestamp')[['current_spread', 'target_spread']]
        # st.line_chart(chart_data)
        
        # Detail metrics
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Avg Spread", f"{market_data['current_spread'].mean():.4f}%")
        c2.metric("Max Spread", f"{market_data['current_spread'].max():.4f}%")
        c3.metric("Avg DWS", f"{market_data['dws'].mean():.4f}%")
        c4.metric("Uptime", f"{(market_data['status'] == 'Healthy').mean()*100:.1f}%")
    else:
        st.info("Collecting historical data... check back in a few hours.")
except Exception as e:
    st.error(f"Error loading history: {e}")

# --- Footer Auto-Refresh ---
st.markdown("---")
st.caption(f"Last UI Refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Auto-refresh logic
from streamlit_autorefresh import st_autorefresh
st_autorefresh(interval=60 * 1000, key="data_refresh")
