import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta, timezone
from streamlit_autorefresh import st_autorefresh

# --- Timezone ---
NIGERIAN_TZ = timezone(timedelta(hours=1))

def get_nigerian_time():
    return datetime.now(NIGERIAN_TZ)

# --- Page config ---
st.set_page_config(
    page_title="Orderbook Health Monitor",
    page_icon="favicon.jpg",
    layout="wide"
)

st_autorefresh(interval=60 * 1000, key="data_refresh")

# --- Severity helpers ---

SEVERITY_ORDER = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2}

def parse_issues(issues_str):
    """
    Parse pipe-separated issue string like 'A1:CRITICAL|A4:MEDIUM'
    into a list of (alert_id, severity) tuples.
    """
    if not issues_str or pd.isna(issues_str) or str(issues_str).strip() == '':
        return []
    parts = [p.strip() for p in str(issues_str).split('|') if ':' in p]
    result = []
    for p in parts:
        alert_id, sev = p.split(':', 1)
        result.append((alert_id.strip(), sev.strip()))
    return result

def worst_severity(issues_str):
    """Return the most severe alert level in an issues string, or ''."""
    issues = parse_issues(issues_str)
    if not issues:
        return ''
    sevs = [s for _, s in issues]
    return min(sevs, key=lambda s: SEVERITY_ORDER.get(s, 99))

def severity_badge(sev):
    """Return an inline HTML badge for a severity level."""
    colours = {
        'CRITICAL': ('🚨', '#ff4444', '#fff'),
        'HIGH':     ('⚠️', '#ff8800', '#fff'),
        'MEDIUM':   ('📊', '#f0b400', '#000'),
    }
    icon, bg, fg = colours.get(sev, ('ℹ️', '#888', '#fff'))
    return (
        f'<span style="background:{bg};color:{fg};padding:1px 7px;'
        f'border-radius:4px;font-size:11px;font-weight:600;">'
        f'{icon} {sev}</span>'
    )

def format_issues_html(issues_str):
    """Render issues string as stacked badges."""
    issues = parse_issues(issues_str)
    if not issues:
        return ''
    badges = [severity_badge(sev) + f'&nbsp;<code style="font-size:11px">{aid}</code>'
              for aid, sev in issues]
    return '<br>'.join(badges)

ALERT_LABELS = {
    'A1': 'Crossed orderbook',
    'A2': 'Spread widening / shallow book',
    'A3': 'One-sided market',
    'A4': 'Thin mid-market',
    'A5': 'Depth imbalance',
}

# --- Data loaders ---

def get_available_log_dates():
    if not os.path.exists('data'):
        return []
    files = sorted(
        [f for f in os.listdir('data') if f.startswith('daily_log_') and f.endswith('.csv')],
        reverse=True
    )
    return [f.replace('daily_log_', '').replace('.csv', '') for f in files]

def load_daily_log(date_str):
    path = os.path.join('data', f'daily_log_{date_str}.csv')
    if not os.path.exists(path):
        return None
    return pd.read_csv(path)

def load_latest():
    path = os.path.join('data', 'latest.csv')
    if not os.path.exists(path):
        return None
    return pd.read_csv(path)

def get_summary_stats(df):
    if df is None or df.empty:
        return None
    status_cols = [c for c in df.columns if c.startswith('STATUS (CHECK')]
    if not status_cols:
        return None

    latest_status_col  = status_cols[-1]
    latest_issues_col  = latest_status_col.replace('STATUS', 'ISSUES')
    status_counts      = df[latest_status_col].value_counts()

    # Count criticals from the latest issues column
    critical_count = 0
    high_count     = 0
    if latest_issues_col in df.columns:
        for issues_str in df[latest_issues_col].dropna():
            ws = worst_severity(issues_str)
            if ws == 'CRITICAL':
                critical_count += 1
            elif ws == 'HIGH':
                high_count += 1

    time_cols = [c for c in df.columns if c.startswith('TIME (CHECK')]
    last_check_time = None
    if time_cols:
        col = time_cols[-1]
        vals = df[col].dropna()
        if not vals.empty:
            last_check_time = vals.iloc[0]

    return {
        'checked':         status_counts.get('CHECKED', 0),
        'warning':         status_counts.get('WARNING', 0),
        'skipped':         status_counts.get('SKIPPED', 0),
        'critical_count':  critical_count,
        'high_count':      high_count,
        'total_checks':    len(status_cols),
        'last_check_time': last_check_time,
    }

# ── Load data ────────────────────────────────────────────────────────────────

available_dates = get_available_log_dates()

st.title("📊 Orderbook Health Monitor")
st.markdown("*Automated hourly monitoring via GitHub Actions*")

if not available_dates:
    st.warning("⚠️ **No data available yet.** The scraper hasn't run yet or the data folder is empty.")
    st.info("Check your GitHub Actions tab to manually trigger a run.")
    st.stop()

today_str    = get_nigerian_time().strftime('%Y-%m-%d')
default_date = today_str if today_str in available_dates else available_dates[0]

selected_date = st.selectbox(
    "📅 Select Date:",
    options=available_dates,
    index=available_dates.index(default_date) if default_date in available_dates else 0,
    format_func=lambda x: (
        f"{datetime.strptime(x, '%Y-%m-%d').strftime('%B %d, %Y')}"
        f"{' (Today)' if x == today_str else ''}"
    )
)

daily_log = load_daily_log(selected_date)
latest_df = load_latest()

if daily_log is None:
    st.error(f"❌ Failed to load log for {selected_date}")
    st.stop()

stats = get_summary_stats(daily_log)

# ── Summary metrics ──────────────────────────────────────────────────────────

st.markdown("---")

m1, m2, m3, m4, m5, m6 = st.columns(6)

with m1:
    st.metric("Total Markets", len(daily_log))
with m2:
    st.metric("✅ Checked", stats['checked'])
with m3:
    val = stats['critical_count']
    st.metric("🚨 Critical", val,
              delta=f"{val} markets" if val else None,
              delta_color="inverse")
with m4:
    val = stats['high_count']
    st.metric("⚠️ High", val,
              delta=f"{val} markets" if val else None,
              delta_color="inverse")
with m5:
    st.metric("⏭️ Skipped", stats['skipped'])
with m6:
    st.metric("🔄 Checks Run", stats['total_checks'],
              delta=f"Last: {stats['last_check_time']}" if stats['last_check_time'] else None)

st.markdown("---")

# ── Daily log table ──────────────────────────────────────────────────────────

st.subheader(f"🎯 Market Status — {datetime.strptime(selected_date, '%Y-%m-%d').strftime('%B %d, %Y')}")

# Build column order: Market → checks descending (STATUS | TIME | ISSUES) → DEPTH
status_cols_all = [c for c in daily_log.columns if c.startswith('STATUS (CHECK')]
check_pairs = []
for sc in status_cols_all:
    num = int(sc.split('CHECK ')[1].rstrip(')'))
    tc  = f'TIME (CHECK {num})'
    ic  = f'ISSUES (CHECK {num})'
    check_pairs.append((num, sc, tc, ic))
check_pairs.sort(reverse=True)   # latest first

ordered_cols = ['Market']
for num, sc, tc, ic in check_pairs:
    ordered_cols.append(sc)
    ordered_cols.append(tc)
    if ic in daily_log.columns:
        ordered_cols.append(ic)
if 'DEPTH' in daily_log.columns:
    ordered_cols.append('DEPTH')

daily_log_display = daily_log[[c for c in ordered_cols if c in daily_log.columns]]

# Identify STATUS and ISSUES cols for styling
status_display_cols = [sc for _, sc, _, _ in check_pairs if sc in daily_log_display.columns]
issues_display_cols = [ic for _, _, _, ic in check_pairs if ic in daily_log_display.columns]

def style_status(val):
    if val == 'WARNING':
        return 'background-color: rgba(255, 50, 50, 0.25); font-weight: bold'
    elif val == 'CHECKED':
        return 'background-color: rgba(50, 205, 50, 0.20)'
    elif val == 'SKIPPED':
        return 'background-color: rgba(255, 165, 0, 0.20)'
    return ''

def style_issues(val):
    ws = worst_severity(val)
    if ws == 'CRITICAL':
        return 'background-color: rgba(255, 0, 0, 0.18); font-size: 11px'
    elif ws == 'HIGH':
        return 'background-color: rgba(255, 136, 0, 0.18); font-size: 11px'
    elif ws == 'MEDIUM':
        return 'background-color: rgba(240, 180, 0, 0.15); font-size: 11px'
    return 'font-size: 11px'

styled = daily_log_display.style
if status_display_cols:
    styled = styled.map(style_status, subset=status_display_cols)
if issues_display_cols:
    styled = styled.map(style_issues, subset=issues_display_cols)

st.dataframe(styled, use_container_width=True, hide_index=True, height=600)

# ── Latest snapshot table (from latest.csv) ──────────────────────────────────

if latest_df is not None and not latest_df.empty:
    st.markdown("---")
    st.subheader("🔬 Latest Run Snapshot")
    st.caption("Detailed metrics from the most recent scraper run.")

    # Columns to show and their display labels
    snapshot_cols = {
        'symbol':          'Market',
        'status':          'Status',
        'issues':          'Issues',
        'current_spread':  'Spread %',
        'ask_layers':      'Ask Layers',
        'bid_layers':      'Bid Layers',
        'depth_1.25x':     'Depth 1.25×',
        'depth_1.5x':      'Depth 1.5×',
        'imbalance_ratio': 'Imbalance',
        'heavier_side':    'Heavy Side',
        'dws':             'DWS',
        'stale_ob_count':  'Stale OB',
    }
    available_snap_cols = [c for c in snapshot_cols if c in latest_df.columns]
    snap = latest_df[available_snap_cols].copy()
    snap.rename(columns={c: snapshot_cols[c] for c in available_snap_cols}, inplace=True)

    def style_snap_status(val):
        v = str(val).upper()
        if v == 'WARNING':
            return 'background-color: rgba(255, 50, 50, 0.25); font-weight: bold'
        elif v == 'CHECKED':
            return 'background-color: rgba(50, 205, 50, 0.20)'
        return ''

    def style_snap_issues(val):
        ws = worst_severity(val)
        if ws == 'CRITICAL': return 'background-color: rgba(255, 0, 0, 0.18)'
        if ws == 'HIGH':     return 'background-color: rgba(255, 136, 0, 0.18)'
        if ws == 'MEDIUM':   return 'background-color: rgba(240, 180, 0, 0.15)'
        return ''

    snap_styled = snap.style
    if 'Status' in snap.columns:
        snap_styled = snap_styled.map(style_snap_status, subset=['Status'])
    if 'Issues' in snap.columns:
        snap_styled = snap_styled.map(style_snap_issues, subset=['Issues'])

    st.dataframe(snap_styled, use_container_width=True, hide_index=True, height=500)

# ── Download ─────────────────────────────────────────────────────────────────

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
with col_b:
    if latest_df is not None:
        st.download_button(
            label="📥 Download Latest Snapshot",
            data=latest_df.to_csv(index=False),
            file_name="latest_snapshot.csv",
            mime="text/csv",
            use_container_width=True
        )

# ── Market details expander ───────────────────────────────────────────────────

st.markdown("---")

with st.expander("📊 Market Details — All Pairs"):

    # Tabs: Warnings first, then full list
    tab_warn, tab_all = st.tabs(["⚠️ Warnings Only", "📋 All Markets"])

    status_cols_sorted = [sc for _, sc, _, _ in check_pairs]   # latest-first order
    latest_status_col  = status_cols_sorted[0] if status_cols_sorted else None
    latest_issues_col  = latest_status_col.replace('STATUS', 'ISSUES') if latest_status_col else None

    def render_market_card(market_row, check_pairs_desc, daily_log_cols):
        market = market_row['Market']
        st.markdown(f"#### {market}")

        # Issue breakdown
        if latest_issues_col and latest_issues_col in daily_log_cols:
            issues_str = market_row.get(latest_issues_col, '')
            issues = parse_issues(issues_str)
            if issues:
                cols = st.columns(len(issues))
                for col, (aid, sev) in zip(cols, issues):
                    label = ALERT_LABELS.get(aid, aid)
                    with col:
                        st.markdown(
                            f'<div style="border-left: 3px solid '
                            f'{"#ff4444" if sev=="CRITICAL" else "#ff8800" if sev=="HIGH" else "#f0b400"}'
                            f';padding: 4px 10px;margin-bottom:6px;">'
                            f'<b>[{aid}]</b> {label}<br>'
                            f'<span style="font-size:11px">{sev}</span></div>',
                            unsafe_allow_html=True
                        )

        # Status progression (latest first, up to 8 checks shown)
        progression_cols = st.columns(min(len(check_pairs_desc), 8))
        for col, (num, sc, tc, ic) in zip(progression_cols, check_pairs_desc[:8]):
            status = market_row.get(sc, '')
            time_v = market_row.get(tc, '')
            issues_v = market_row.get(ic, '') if ic in daily_log_cols else ''
            icon = "🚨" if worst_severity(issues_v) == 'CRITICAL' else \
                   "⚠️" if status == 'WARNING' else \
                   "✅" if status == 'CHECKED' else "⏭️"
            with col:
                st.markdown(
                    f'<div style="text-align:center;font-size:12px;">'
                    f'<div style="font-size:20px">{icon}</div>'
                    f'<div><b>#{num}</b></div>'
                    f'<div style="color:#888">{time_v}</div>'
                    f'</div>',
                    unsafe_allow_html=True
                )

        # Latest snapshot metrics (from latest.csv)
        if latest_df is not None:
            snap_row = latest_df[latest_df['symbol'] == market]
            if not snap_row.empty:
                snap = snap_row.iloc[0]
                c1, c2, c3, c4, c5 = st.columns(5)
                with c1:
                    st.metric("Spread", f"{snap.get('current_spread', 'N/A')}%")
                with c2:
                    st.metric("Depth 1.25×", snap.get('depth_1.25x', 'N/A'))
                with c3:
                    st.metric("Ask / Bid layers",
                              f"{int(snap.get('ask_layers', 0))} / {int(snap.get('bid_layers', 0))}")
                with c4:
                    ratio = snap.get('imbalance_ratio', '')
                    side  = snap.get('heavier_side', '')
                    st.metric("Imbalance", f"{ratio}×" if ratio not in ('', 'inf') else ratio,
                              delta=side if side else None)
                with c5:
                    stale = snap.get('stale_ob_count', 0)
                    st.metric("Stale OB runs", int(stale) if stale == stale else 0)

        if 'DEPTH' in market_row and market_row['DEPTH']:
            st.caption(f"📊 Depth (1.25× / 1.5×): {market_row['DEPTH']}")

        st.markdown("---")

    with tab_warn:
        if latest_status_col:
            warning_rows = daily_log[daily_log[latest_status_col] == 'WARNING']
            if warning_rows.empty:
                st.success("✅ No markets in warning state on the latest check.")
            else:
                for _, row in warning_rows.iterrows():
                    render_market_card(row, check_pairs, daily_log.columns)
        else:
            st.info("No check data available.")

    with tab_all:
        # Sort: warnings first, then checked, then skipped
        def sort_key(row):
            s = row.get(latest_status_col, '') if latest_status_col else ''
            return {'WARNING': 0, 'CHECKED': 1, 'SKIPPED': 2}.get(s, 3)

        sorted_rows = sorted(daily_log.itertuples(index=False), key=lambda r: sort_key(r._asdict()))
        for row in sorted_rows:
            render_market_card(dict(zip(daily_log.columns, row)), check_pairs, daily_log.columns)

# ── Footer ────────────────────────────────────────────────────────────────────

nigerian_now = get_nigerian_time()
st.caption(
    f"Dashboard Time: {nigerian_now.strftime('%Y-%m-%d %H:%M:%S WAT')} "
    f"• Auto-refresh: 60s"
)
