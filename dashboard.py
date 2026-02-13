import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# Nigerian timezone (UTC+1)
NIGERIAN_TZ = timezone(timedelta(hours=1))

def get_nigerian_time():
    """Returns current time in Nigerian timezone (UTC+1)"""
    return datetime.now(NIGERIAN_TZ)

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

def get_available_log_dates():
    """Get list of available daily log dates."""
    if not os.path.exists('data'):
        return []
    
    log_files = sorted([f for f in os.listdir('data') if f.startswith('daily_log_') and f.endswith('.csv')], reverse=True)
    return [f.replace('daily_log_', '').replace('.csv', '') for f in log_files]

def load_daily_log(date_str):
    """Load daily log for a specific date."""
    log_path = os.path.join('data', f'daily_log_{date_str}.csv')
    
    if not os.path.exists(log_path):
        return None
    
    return pd.read_csv(log_path)

def get_summary_stats(df):
    """Extract summary statistics from the daily log."""
    if df is None or df.empty:
        return None
    
    # Get all status columns
    status_cols = [col for col in df.columns if col.startswith('STATUS (CHECK')]
    
    if not status_cols:
        return None
    
    # Use the most recent check (last status column)
    latest_status_col = status_cols[-1]
    
    # Count statuses
    status_counts = df[latest_status_col].value_counts()
    
    checked = status_counts.get('CHECKED', 0)
    warning = status_counts.get('WARNING', 0)
    skipped = status_counts.get('SKIPPED', 0)
    
    # Get number of checks
    num_checks = len(status_cols)
    
    # Get last check time
    time_cols = [col for col in df.columns if col.startswith('TIME (CHECK')]
    if time_cols:
        latest_time_col = time_cols[-1]
        # Get first non-empty time value
        last_check_time = df[latest_time_col].dropna().iloc[0] if not df[latest_time_col].dropna().empty else None
    else:
        last_check_time = None
    
    return {
        'checked': checked,
        'warning': warning,
        'skipped': skipped,
        'total_checks': num_checks,
        'last_check_time': last_check_time
    }

# --- Main Dashboard ---

# Get available dates
available_dates = get_available_log_dates()

if not available_dates:
    st.warning("⚠️ **No data available yet.** The scraper hasn't run yet or the data folder is empty.")
    st.info("Check your GitHub Actions tab to manually trigger a run.")
    st.stop()

# Date Selection
today_str = get_nigerian_time().strftime('%Y-%m-%d')
default_date = today_str if today_str in available_dates else available_dates[0]

col1, col2 = st.columns([3, 1])

with col1:
    selected_date = st.selectbox(
        "📅 Select Date:",
        options=available_dates,
        index=available_dates.index(default_date) if default_date in available_dates else 0,
        format_func=lambda x: f"{datetime.strptime(x, '%Y-%m-%d').strftime('%B %d, %Y')} {'(Today)' if x == today_str else ''}"
    )

# Load selected log
daily_log = load_daily_log(selected_date)

if daily_log is None:
    st.error(f"❌ Failed to load log for {selected_date}")
    st.stop()

# Get summary stats
stats = get_summary_stats(daily_log)

# --- Summary Metrics ---
st.markdown("---")

m1, m2, m3, m4, m5 = st.columns(5)

with m1:
    st.metric("Total Markets", len(daily_log))

with m2:
    st.metric("✅ Checked", stats['checked'], delta=None)

with m3:
    st.metric("⚠️ Warnings", stats['warning'], 
              delta=None if stats['warning'] == 0 else f"{stats['warning']} issues",
              delta_color="inverse")

with m4:
    st.metric("⏭️ Skipped", stats['skipped'], delta=None)

with m5:
    st.metric("🔄 Total Checks", stats['total_checks'], 
              delta=f"Last: {stats['last_check_time']}" if stats['last_check_time'] else None)

st.markdown("---")

# --- Daily Log Table ---
st.subheader(f"🎯 Market Status - {datetime.strptime(selected_date, '%Y-%m-%d').strftime('%B %d, %Y')}")

def style_status_cell(val):
    """Style individual status cells with colors."""
    if val == 'WARNING':
        return 'background-color: rgba(255, 50, 50, 0.25); font-weight: bold'
    elif val == 'CHECKED':
        return 'background-color: rgba(50, 255, 50, 0.20)'
    elif val == 'SKIPPED':
        return 'background-color: rgba(255, 165, 0, 0.20)'
    return ''

# Apply styling to all STATUS columns
status_cols = [col for col in daily_log.columns if col.startswith('STATUS (CHECK')]

styled_df = daily_log.style.applymap(
    style_status_cell,
    subset=status_cols
)

st.dataframe(
    styled_df,
    use_container_width=True,
    hide_index=True,
    height=600
)

# --- Download Section ---
st.markdown("---")

col_a, col_b = st.columns([1, 3])

with col_a:
    csv = daily_log.to_csv(index=False)
    st.download_button(
        label=f"📥 Download {selected_date} Log",
        data=csv,
        file_name=f"daily_log_{selected_date}.csv",
        mime="text/csv",
        use_container_width=True
    )

# --- Footer ---
nigerian_now = get_nigerian_time()
st.caption(f"Dashboard Time: {nigerian_now.strftime('%Y-%m-%d %H:%M:%S WAT')} • Auto-refresh: 60s")

# --- Market Details Section (Optional Expandable) ---
st.markdown("---")

with st.expander("📊 View Market Details"):
    # Filter for markets with warnings in the latest check
    if stats and stats['warning'] > 0:
        latest_status_col = status_cols[-1]
        warning_markets = daily_log[daily_log[latest_status_col] == 'WARNING']['Market'].tolist()
        
        if warning_markets:
            st.markdown("### ⚠️ Markets Currently in Warning State:")
            for market in warning_markets:
                market_row = daily_log[daily_log['Market'] == market].iloc[0]
                
                st.markdown(f"**{market}** (Target: {market_row['% Spd']})")
                
                # Show status progression across checks
                status_progression = []
                for i, status_col in enumerate(status_cols, 1):
                    status = market_row[status_col]
                    time_col = f'TIME (CHECK {i})'
                    time_val = market_row[time_col] if time_col in market_row else 'N/A'
                    
                    emoji = "⚠️" if status == "WARNING" else "✅" if status == "CHECKED" else "⏭️"
                    status_progression.append(f"{emoji} Check {i} ({time_val}): {status}")
                
                st.markdown("  \n".join(status_progression))
                
                # Show depth if available
                if 'DEPTH' in market_row:
                    st.markdown(f"📊 Liquidity Depth: {market_row['DEPTH']}")
                
                st.markdown("---")
    else:
        st.success("✅ All markets are healthy!")
