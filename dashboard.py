import streamlit as st
import pandas as pd
import os
import json
import glob
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh

# --- Page Configuration ---
st.set_page_config(
    page_title="Orderbook Health Monitor",
    page_icon="📊",
    layout="wide"
)

# Auto-refresh every 60 seconds
st_autorefresh(interval=60 * 1000, key="data_refresh")

st.title("📊 Orderbook Health Monitor")
st.markdown("*Automated hourly monitoring via GitHub Actions*")

# --- Helper Functions ---

def load_latest_data():
    """Load current status from latest.csv and consecutive strikes from JSON."""
    if not os.path.exists('data/latest.csv'):
        return None
    
    df = pd.read_csv('data/latest.csv')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def load_24h_history():
    """Load today and yesterday's CSV files to construct a 24h window."""
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    files_to_load = [
        os.path.join('data', f"{yesterday.strftime('%Y_%m_%d')}.csv"),
        os.path.join('data', f"{today.strftime('%Y_%m_%d')}.csv")
    ]
    
    dataframes = []
    for f in files_to_load:
        if os.path.exists(f):
            try:
                dataframes.append(pd.read_csv(f))
            except Exception:
                pass
            
    if not dataframes:
        return pd.DataFrame()
    
    combined = pd.concat(dataframes).drop_duplicates()
    combined['timestamp'] = pd.to_datetime(combined['timestamp'])
    
    # Filter for the last 24 hours only
    cutoff = datetime.now() - timedelta(hours=24)
    return combined[combined['timestamp'] >= cutoff]

# --- Main Dashboard Logic ---

latest_df = load_latest_data()

if latest_df is None:
    st.warning("⚠️ **No data available yet.** The scraper hasn't run yet or the data folder is empty.")
    st.info("Check your GitHub Actions tab to manually trigger a run.")
    st.stop()

# --- Top Metrics Row ---
last_update = latest_df['timestamp'].max()
time_diff = datetime.now() - last_update
minutes_ago = int(time_diff.total_seconds() / 60)

m1, m2, m3, m4 = st.columns(4)

with m1:
    st.metric("Last Update", last_update.strftime('%H:%M:%S'))

with m2:
    healthy_count = (latest_df['status'] == 'Healthy').sum()
    st.metric("Healthy Markets", healthy_count)

with m3:
    warning_count = (latest_df['status'] == 'Warning').sum()
    st.metric("Warning Markets", warning_count, 
              delta=None if warning_count == 0 else f"{warning_count} issues",
              delta_color="inverse")

with m4:
    status_label = "Live" if minutes_ago < 90 else "Stale"
    st.metric("Monitor Status", status_label, delta=f"{minutes_ago}m ago")

st.markdown("---")

# --- Current Status Table ---
st.subheader("🎯 Current Market Status")

# Select and rename columns for display
# Based on your scraper output: timestamp, symbol, status, strikes, current_spread, target_spread, percent_diff, dws, depth_1pct_display
display_df = latest_df[[
    'symbol', 'status', 'strikes', 'current_spread', 
    'target_spread', 'percent_diff', 'dws', 'depth_1pct_display'
]].copy()

display_df.columns = [
    'Market', 'Status', 'Strikes', 'Spread %', 
    'Target %', 'Diff %', 'DWS %', 'Depth'
]

def style_status(row):
    """Apply RGBA colors for transparency (Theme-Aware)"""
    if row['Status'] == 'Warning':
        return ['background-color: rgba(255, 50, 50, 0.25)'] * len(row)
    elif row['Status'] == 'Healthy':
        return ['background-color: rgba(50, 255, 50, 0.20)'] * len(row)
    return [''] * len(row)

st.dataframe(
    display_df.style.apply(style_status, axis=1),
    use_container_width=True,
    hide_index=True
)

# --- Historical Trends Section ---
st.markdown("---")
st.subheader("📈 24h Spread Trends")

hist_df = load_24h_history()

if not hist_df.empty:
    markets = sorted(hist_df['symbol'].unique())
    selected_market = st.selectbox("Select Market to Analyze", options=markets)
    
    market_data = hist_df[hist_df['symbol'] == selected_market].sort_values('timestamp')
    
    # Spread Line Chart
    chart_data = market_data.set_index('timestamp')[['current_spread', 'target_spread']]
    st.line_chart(chart_data)
    
    # Statistical Summary for Market
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Avg Spread", f"{market_data['current_spread'].mean():.4f}%")
    c2.metric("Max Spread", f"{market_data['current_spread'].max():.4f}%")
    c3.metric("Avg DWS", f"{market_data['dws'].mean():.4f}%")
    
    uptime = (market_data['status'] == 'Healthy').mean() * 100
    c4.metric("Uptime %", f"{uptime:.1f}%")

else:
    st.info("Collecting historical logs... History will populate as daily CSV files are created.")

# --- System Information ---
st.markdown("---")
with st.expander("ℹ️ System Information"):
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("""
        **Data Storage:**
        - `latest.csv`: Real-time state.
        - `YYYY_MM_DD.csv`: Rolling daily logs.
        - `health_state.json`: Strike tracking.
        """)
    with col_b:
        st.markdown("""
        **Metrics Info:**
        - **DWS:** Dollar-Weighted Spread.
        - **Strikes:** Consecutive hours of Warning status.
        - **Uptime:** % of checks where status was 'Healthy'.
        """)

# Footer
st.caption(f"Last UI Refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} • Auto-refresh active")
