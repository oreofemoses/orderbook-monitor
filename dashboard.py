import streamlit as st
import pandas as pd
import os
from datetime import datetime
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

# --- Load Data ---

def load_latest_data():
    """Load current status from latest.csv."""
    if not os.path.exists('data/latest.csv'):
        return None
    
    df = pd.read_csv('data/latest.csv')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

latest_df = load_latest_data()

if latest_df is None:
    st.warning("⚠️ **No data available yet.** The scraper hasn't run yet or the data folder is empty.")
    st.info("Check your GitHub Actions tab to manually trigger a run.")
    st.stop()

# --- Summary Metrics ---
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

# --- Market Status Table ---
st.subheader("🎯 Current Market Status")

# Select and rename columns for display
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

# Footer
st.caption(f"Last UI Refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} • Auto-refresh active")
