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

st.markdown("---")

# --- Daily Event Logs Section ---
st.subheader("📅 Daily Event Logs")

col1, col2 = st.columns([2, 1])

with col1:
    # Get list of available daily log files
    log_files = sorted([f for f in os.listdir('data') if f.startswith('daily_log_') and f.endswith('.csv')], reverse=True)
    
    if log_files:
        # Extract dates from filenames
        available_dates = [f.replace('daily_log_', '').replace('.csv', '') for f in log_files]
        
        # Date picker
        selected_date = st.selectbox(
            "Select date to view events:",
            options=available_dates,
            format_func=lambda x: datetime.strptime(x, '%Y-%m-%d').strftime('%B %d, %Y')
        )
        
        if selected_date:
            log_path = os.path.join('data', f'daily_log_{selected_date}.csv')
            
            if os.path.exists(log_path):
                log_df = pd.read_csv(log_path)
                
                # Display event count summary
                event_counts = log_df['event_type'].value_counts()
                
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    st.metric("Total Events", len(log_df))
                with col_b:
                    warnings_entered = event_counts.get('WARNING_ENTERED', 0)
                    st.metric("Warnings Entered", warnings_entered)
                with col_c:
                    warnings_cleared = event_counts.get('WARNING_CLEARED', 0)
                    st.metric("Warnings Cleared", warnings_cleared)
                
                # Display the log table
                st.dataframe(log_df, use_container_width=True, hide_index=True)
                
                # Download button
                csv = log_df.to_csv(index=False)
                st.download_button(
                    label=f"📥 Download {selected_date} Log",
                    data=csv,
                    file_name=f"daily_log_{selected_date}.csv",
                    mime="text/csv"
                )
            else:
                st.warning(f"Log file not found for {selected_date}")
    else:
        st.info("No daily event logs available yet. Logs will appear after the first scraper run.")
