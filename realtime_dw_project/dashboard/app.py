"""电商实时数仓运营监控看板 — Streamlit Dashboard"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import urllib.request
import urllib.parse
import json
import base64
from datetime import datetime

st.set_page_config(page_title="电商实时数仓看板", page_icon="", layout="wide")
st.title("E-commerce Real-Time Analytics Dashboard")
st.caption(f"Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

CH_URL = "http://localhost:8123"
CH_USER = "default"
CH_PASSWORD = "clickhouse"


def query(sql):
    params = urllib.parse.urlencode({"query": sql, "default_format": "JSONEachRow"})
    auth = base64.b64encode(f"{CH_USER}:{CH_PASSWORD}".encode("utf-8")).decode("ascii")
    req = urllib.request.Request(f"{CH_URL}/?{params}", headers={"Authorization": f"Basic {auth}"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read().decode("utf-8").strip()
            df = pd.DataFrame([json.loads(line) for line in data.split("\n") if line])
            for col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col])
                except (ValueError, TypeError):
                    pass
            return df
    except Exception as e:
        st.error(f"ClickHouse query failed: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=10)
def load_overview():
    return query("SELECT * FROM ads_realtime_overview ORDER BY window_start DESC LIMIT 50")


@st.cache_data(ttl=10)
def load_product_rank():
    return query("SELECT * FROM ads_product_rank ORDER BY window_start DESC, pay_amount DESC LIMIT 100")


@st.cache_data(ttl=10)
def load_category_rank():
    return query("SELECT * FROM ads_category_rank ORDER BY window_start DESC, pay_amount DESC LIMIT 50")


@st.cache_data(ttl=10)
def load_channel_funnel():
    return query("SELECT * FROM ads_channel_funnel ORDER BY window_start DESC LIMIT 100")


@st.cache_data(ttl=10)
def load_alerts():
    return query("SELECT * FROM ads_realtime_alert ORDER BY created_at DESC LIMIT 50")


@st.cache_data(ttl=10)
def load_dwd_sample():
    return query("SELECT * FROM dwd_user_behavior ORDER BY event_ts DESC LIMIT 1000")


# ---- Sidebar ----
st.sidebar.header("Filters")
auto_refresh = st.sidebar.checkbox("Auto refresh (10s)", value=False)
if auto_refresh:
    st.rerun()

# ---- Row 1: KPI Cards ----
st.subheader("Real-Time Overview KPIs")
ov = load_overview()
if not ov.empty:
    latest = ov.iloc[0]
    pv = int(latest['pv'])
    uv = int(latest['uv'])
    cu = int(latest['cart_users'])
    ou = int(latest['order_users'])
    pu = int(latest['pay_users'])
    pa = float(latest['pay_amount'])
    cols = st.columns(6)
    cols[0].metric("PV", f"{pv:,}")
    cols[1].metric("UV", f"{uv:,}")
    cols[2].metric("Cart Users", f"{cu:,}")
    cols[3].metric("Order Users", f"{ou:,}")
    cols[4].metric("Pay Users", f"{pu:,}")
    cols[5].metric("Pay Amount", f"{pa:,.2f}")
else:
    st.warning("No overview data yet.")

st.divider()

# ---- Row 2: Pay Amount Trend + Product Rank ----
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Pay Amount Trend (1-min windows)")
    if not ov.empty and len(ov) > 1:
        df = ov.sort_values("window_start").tail(30)
        fig = px.line(df, x="window_start", y="pay_amount", markers=True,
                       labels={"window_start": "Time", "pay_amount": "Pay Amount"})
        fig.update_layout(height=350, margin=dict(l=20, r=20, t=10, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Need more data for trend chart.")

with col_right:
    st.subheader("Top 10 Products by Pay Amount")
    pr = load_product_rank()
    if not pr.empty:
        top10 = pr.groupby("product_name")["pay_amount"].sum().nlargest(10).reset_index()
        fig = px.bar(top10, x="pay_amount", y="product_name", orientation="h",
                      labels={"pay_amount": "Total Pay Amount", "product_name": ""})
        fig.update_layout(height=350, margin=dict(l=20, r=20, t=10, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No product rank data yet.")

# ---- Row 3: Category Sales + Channel Funnel ----
col_left2, col_right2 = st.columns(2)

with col_left2:
    st.subheader("Category Sales Distribution")
    cr = load_category_rank()
    if not cr.empty:
        cat_agg = cr.groupby("category_name")["pay_amount"].sum().reset_index()
        fig = px.pie(cat_agg, values="pay_amount", names="category_name",
                      labels={"pay_amount": "Pay Amount", "category_name": "Category"})
        fig.update_layout(height=350, margin=dict(l=20, r=20, t=10, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No category rank data yet.")

with col_right2:
    st.subheader("Channel Conversion Funnel")
    cf = load_channel_funnel()
    if not cf.empty:
        ch_agg = cf.groupby("channel")[["view_users", "cart_users", "order_users", "pay_users"]].sum()
        fig = go.Figure()
        channels = ch_agg.index.tolist()
        for col_name, color in [("view_users", "#636EFA"), ("cart_users", "#00CC96"),
                                  ("order_users", "#AB63FA"), ("pay_users", "#FFA15A")]:
            fig.add_trace(go.Bar(name=col_name.replace("_users", ""), y=channels,
                                  x=ch_agg[col_name], orientation="h", marker_color=color))
        fig.update_layout(barmode="group", height=350, margin=dict(l=20, r=20, t=10, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No channel funnel data yet.")

# ---- Row 4: Alerts Table ----
st.divider()
st.subheader("Real-Time Alerts")
alerts = load_alerts()
if not alerts.empty:
    st.dataframe(alerts.sort_values("created_at", ascending=False).head(20),
                 use_container_width=True, hide_index=True)
else:
    st.info("No alerts generated yet. Run scripts/generate_alerts.py to populate.")

# ---- Row 5: Raw DWD Sample ----
st.divider()
st.subheader("DWD Detail Sample (latest 20 rows)")
dwd = load_dwd_sample()
if not dwd.empty:
    st.dataframe(dwd.head(20), use_container_width=True, hide_index=True)
else:
    st.info("No DWD data yet.")
