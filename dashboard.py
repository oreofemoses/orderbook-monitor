import streamlit as st
import pandas as pd
import glob
from datetime import datetime, timedelta
import os

st.set_page_config(
    page_title="Orderbook Health Monitor",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Orderbook Health Monitor")
st.markdown("*Automated hourly monitoring via GitHub Actions*")

# Check if data exists
if not os.path.exists('data/latest.csv'):
    st.warning("""
    ⚠️ **No data available yet**
    
    The GitHub Actions scraper hasn't run yet, or the data folder is empty.
    
    To trigger a manual run:
    1. Go to your GitHub repository
    2. Click "Actions" tab
    3. Click "Hourly Orderbook Scraper"
    4. Click "Run workflow"
    
    Data should appear here within 2-3 minutes.
    """)
    st.stop()

# Load latest data
try:
    latest_df = pd.read_csv('data/latest.csv')
    latest_df['timestamp'] = pd.to_datetime(latest_df['timestamp'])
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

# Display last update time
last_update = latest_df['timestamp'].max()
time_ago = datetime.now() - last_update
minutes_ago = int(time_ago.total_seconds() / 60)

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Last Update", last_update.strftime('%H:%M:%S'))

with col2:
    healthy_count = (latest_df['status'] == 'Healthy').sum()
    st.metric("Healthy Markets", healthy_count, 
             delta=None, 
             delta_color="normal")

with col3:
    warning_count = (latest_df['status'] == 'Warning').sum()
    st.metric("Warning Markets", warning_count,
             delta=None if warning_count == 0 else f"{warning_count} issues",
             delta_color="inverse")

with col4:
    st.metric("Minutes Since Update", minutes_ago)

st.markdown("---")

# Current Status Table
st.subheader("🎯 Current Market Status")

# Prepare display dataframe
display_df = latest_df[[
    'symbol', 'status', 'current_spread', 'target_spread', 
    'percent_diff', 'depth_1pct_display', 'depth_2pct_display', 'dws'
]].copy()

display_df.columns = [
    'Market', 'Status', 'Current Spread', 'Target', 
    '% Diff', '1% Depth', '2% Depth', 'DWS'
]

# Color-code status
def highlight_status(row):
    if row['Status'] == 'Warning':
        return ['background-color: #ffebee'] * len(row)
    elif row['Status'] == 'Healthy':
        return ['background-color: #e8f5e9'] * len(row)
    else:
        return ['background-color: #fff9c4'] * len(row)

st.dataframe(
    display_df.style.apply(highlight_status, axis=1),
    use_container_width=True,
    hide_index=True
)

# Historical Data Section
st.markdown("---")
st.subheader("📈 Historical Trends")

# Load historical data
try:
    all_files = sorted(glob.glob('data/orderbook_*.csv'))
    
    if len(all_files) > 1:
        # Load last 24 files (24 hours of data)
        recent_files = all_files[-24:]
        
        historical_dfs = []
        for file in recent_files:
            df = pd.read_csv(file)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            historical_dfs.append(df)
        
        historical_df = pd.concat(historical_dfs, ignore_index=True)
        
        # Create tabs for each market
        markets = historical_df['symbol'].unique()
        tabs = st.tabs(markets)
        
        for i, market in enumerate(markets):
            with tabs[i]:
                market_data = historical_df[historical_df['symbol'] == market].copy()
                market_data = market_data.sort_values('timestamp')
                
                # Spread chart
                st.markdown(f"**Spread Trend - {market}**")
                
                chart_df = market_data[['timestamp', 'current_spread', 'target_spread']].copy()
                chart_df = chart_df.set_index('timestamp')
                
                st.line_chart(chart_df)
                
                # Stats
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    avg_spread = market_data['current_spread'].mean()
                    st.metric("Avg Spread (24h)", f"{avg_spread:.6f}")
                
                with col2:
                    avg_diff = market_data['percent_diff'].mean()
                    st.metric("Avg Deviation", f"{avg_diff:+.2f}%")
                
                with col3:
                    warning_pct = (market_data['status'] == 'Warning').sum() / len(market_data) * 100
                    st.metric("Warning %", f"{warning_pct:.1f}%")
    
    else:
        st.info("Need at least 2 hourly checks for historical trends. Check back soon!")

except Exception as e:
    st.warning(f"Unable to load historical data: {e}")

# Info Section
st.markdown("---")
st.subheader("ℹ️ System Info")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    **How it works:**
    - 🔄 GitHub Actions runs scraper every hour
    - 📊 Data automatically committed to repository
    - 📱 Telegram alerts sent for warnings
    - 🌐 This dashboard reads from the data folder
    
    **Next check:** Within the next hour (at :00)
    """)

with col2:
    st.markdown("""
    **Alert Thresholds:**
    - ⚠️ Warning: Spread >50% from target
    - ✅ Healthy: Spread within 50% of target
    - 📊 Monitors: Spread, DWS, Liquidity Depth
    
    **Telegram Commands:**
    - Check GitHub Actions tab to manually trigger
    """)

# Footer
st.markdown("---")
st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} • Auto-refresh every 60 seconds")

# Auto-refresh every 60 seconds
st_autorefresh = st.empty()
with st_autorefresh:
    st.markdown("""
    <meta http-equiv="refresh" content="60">
    """, unsafe_allow_html=True)
