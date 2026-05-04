import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pyarrow.parquet as pq
import requests
import io
import os

st.set_page_config(
    page_title="BDS Analytics — Bất Động Sản VN",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

WEBHDFS = "http://namenode:9870/webhdfs/v1"

# ── Màu sắc ──────────────────────────────────────────────────
COLORS = {
    "Nhà phố/Vila":       "#1db954",
    "Căn hộ chung cư":    "#ff6b6b",
    "Đất nền":            "#ffd700",
    "Biệt thự/liền kề":  "#a78bfa",
    "Nhà riêng":          "#38bdf8",
}

TIER_ORDER = ["Dưới 2 tỷ", "2-5 tỷ", "5-10 tỷ", "10-20 tỷ", "Trên 20 tỷ"]

# ── Load Data ─────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_batch(filename: str) -> pd.DataFrame:
    for prefix in ["/data/", "data/", "../data/"]:
        p = prefix + filename
        if os.path.exists(p):
            try:
                return pd.read_parquet(p)
            except Exception:
                continue
    return pd.DataFrame()


@st.cache_data(ttl=5)
def load_streaming() -> pd.DataFrame:
    try:
        path = "/realestate/streaming/listings"
        url  = f"{WEBHDFS}{path}?op=LISTSTATUS"
        resp = requests.get(url, timeout=3)
        resp.raise_for_status()
        files = resp.json()["FileStatuses"]["FileStatus"]
        parquet_files = [f for f in files if ".parquet" in f["pathSuffix"]]
        if not parquet_files:
            return pd.DataFrame()
        dfs = []
        for f in sorted(parquet_files, key=lambda x: x["modificationTime"], reverse=True)[:8]:
            file_url = f"{WEBHDFS}{path}/{f['pathSuffix']}?op=OPEN"
            r = requests.get(file_url, allow_redirects=True, timeout=5)
            dfs.append(pq.read_table(io.BytesIO(r.content)).to_pandas())
        return pd.concat(dfs, ignore_index=True)
    except Exception:
        return pd.DataFrame()


# ── Load tất cả datasets ──────────────────────────────────────
df_cleaned    = load_batch("cleaned.parquet")
df_district   = load_batch("by_district.parquet")
df_type       = load_batch("by_type.parquet")
df_price_dist = load_batch("price_dist.parquet")
df_cluster    = load_batch("cluster_stats.parquet")
df_legal      = load_batch("by_legal.parquet")
df_direction  = load_batch("by_direction.parquet")
df_stream     = load_streaming()

# ── Sidebar filter ────────────────────────────────────────────
with st.sidebar:
    st.title("🏠 BDS Analytics")
    st.caption("Thị trường bất động sản Việt Nam")
    st.divider()

    prop_types = ["Tất cả"]
    if not df_cleaned.empty and "property_type" in df_cleaned.columns:
        prop_types += sorted(df_cleaned["property_type"].dropna().unique().tolist())
    selected_type = st.selectbox("Loại BĐS", prop_types)

    cities = ["Tất cả"]
    if not df_cleaned.empty and "city" in df_cleaned.columns:
        cities += sorted(df_cleaned["city"].dropna().unique().tolist())
    selected_city = st.selectbox("Thành phố", cities)

    if not df_cleaned.empty and "price_billion" in df_cleaned.columns:
        min_p = float(df_cleaned["price_billion"].min() or 0)
        max_p = float(df_cleaned["price_billion"].quantile(0.95) or 50)
        price_range = st.slider("Khoảng giá (tỷ VND)", min_p, max_p, (min_p, max_p), step=0.5)
    else:
        price_range = (0, 50)

    st.divider()
    st.caption("Dữ liệu từ batdongsan.com")

# ── Áp filter ─────────────────────────────────────────────────
df_filt = df_cleaned.copy() if not df_cleaned.empty else pd.DataFrame()
if not df_filt.empty:
    if selected_type != "Tất cả" and "property_type" in df_filt.columns:
        df_filt = df_filt[df_filt["property_type"] == selected_type]
    if selected_city != "Tất cả" and "city" in df_filt.columns:
        df_filt = df_filt[df_filt["city"] == selected_city]
    if "price_billion" in df_filt.columns:
        df_filt = df_filt[
            (df_filt["price_billion"] >= price_range[0]) &
            (df_filt["price_billion"] <= price_range[1])
        ]

# ── Header ────────────────────────────────────────────────────
col_h1, col_h2 = st.columns([4, 1])
with col_h1:
    st.title("🏠 BDS Analytics — Thị trường Bất Động Sản")
    st.caption("Phân tích dữ liệu thời gian thực từ batdongsan.com · Lambda Architecture")
with col_h2:
    if not df_stream.empty:
        st.success("🟢 LIVE STREAM ACTIVE")
    else:
        st.warning("🟡 Chờ stream...")

st.divider()

# ── KPI Cards ─────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Tổng tin đăng", f"{len(df_filt):,}" if not df_filt.empty else "—")

if not df_filt.empty and "price_billion" in df_filt.columns:
    avg_p = df_filt["price_billion"].mean()
    k2.metric("Giá TB", f"{avg_p:.2f} tỷ")
else:
    k2.metric("Giá TB", "—")

if not df_filt.empty and "price_per_m2_million" in df_filt.columns:
    avg_m2 = df_filt["price_per_m2_million"].mean()
    k3.metric("Giá TB/m²", f"{avg_m2:.0f} tr/m²")
else:
    k3.metric("Giá TB/m²", "—")

if not df_filt.empty and "district" in df_filt.columns:
    top_dist = df_filt["district"].value_counts().idxmax()
    k4.metric("Quận nhiều nhất", top_dist)
else:
    k4.metric("Quận nhiều nhất", "—")

k5.metric("Tin real-time", f"+{len(df_stream):,}" if not df_stream.empty else "0")

st.write("")

# ── Row 1: Giá theo quận & Phân phối loại BĐS ────────────────
row1_c1, row1_c2 = st.columns(2)

with row1_c1:
    st.subheader("Giá trung bình theo Quận/Huyện")
    st.caption("Top 15 quận có nhiều tin đăng nhất")

    src = df_district if not df_district.empty else (
        df_filt.groupby("district").agg(
            avg_price_billion=("price_billion", "mean"),
            total_listings=("listing_id", "count"),
        ).reset_index() if not df_filt.empty and "district" in df_filt.columns else pd.DataFrame()
    )

    if not src.empty:
        price_col = "avg_price_billion" if "avg_price_billion" in src.columns else "avg_price"
        src = src.dropna(subset=[price_col]).nlargest(15, "total_listings")
        fig = px.bar(
            src, x="district", y=price_col,
            color=price_col,
            color_continuous_scale="Viridis",
            labels={"district": "Quận/Huyện", price_col: "Giá TB (tỷ VND)"},
        )
        fig.update_layout(coloraxis_showscale=False, xaxis_tickangle=-35)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu quận.")

with row1_c2:
    st.subheader("Phân bổ theo Loại BĐS")
    st.caption("Số tin đăng theo từng phân khúc")

    src_type = df_type if not df_type.empty else (
        df_filt.groupby("property_type").size().reset_index(name="total_listings")
        if not df_filt.empty and "property_type" in df_filt.columns else pd.DataFrame()
    )

    if not src_type.empty:
        fig_pie = px.pie(
            src_type, names="property_type", values="total_listings",
            color="property_type",
            color_discrete_map=COLORS,
            hole=0.4,
        )
        fig_pie.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu loại BĐS.")

st.divider()

# ── Row 2: Phân phối giá & Giá/m² theo loại ──────────────────
row2_c1, row2_c2 = st.columns(2)

with row2_c1:
    st.subheader("Phân phối Khoảng Giá")
    st.caption("Tỷ lệ tin đăng theo phân khúc giá")

    src_pd = df_price_dist if not df_price_dist.empty else (
        df_filt.groupby("price_tier").size().reset_index(name="count")
        if not df_filt.empty and "price_tier" in df_filt.columns else pd.DataFrame()
    )

    if not src_pd.empty and "price_tier" in src_pd.columns:
        src_pd["price_tier"] = pd.Categorical(src_pd["price_tier"], categories=TIER_ORDER, ordered=True)
        src_pd = src_pd.sort_values("price_tier")
        fig_tier = px.bar(
            src_pd, x="price_tier", y="count",
            color="price_tier",
            color_discrete_sequence=px.colors.sequential.Plasma_r,
            labels={"price_tier": "Phân khúc giá", "count": "Số tin đăng"},
        )
        fig_tier.update_layout(showlegend=False)
        st.plotly_chart(fig_tier, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu phân phối giá.")

with row2_c2:
    st.subheader("Giá/m² trung bình theo Loại BĐS")
    st.caption("So sánh hiệu quả đầu tư giữa các loại hình")

    src_m2 = df_type if not df_type.empty else (
        df_filt.groupby("property_type")["price_per_m2_million"].mean().reset_index()
        if not df_filt.empty and "property_type" in df_filt.columns else pd.DataFrame()
    )

    if not src_m2.empty:
        price_m2_col = "avg_price_per_m2" if "avg_price_per_m2" in src_m2.columns else "price_per_m2_million"
        src_m2 = src_m2.dropna(subset=[price_m2_col]).sort_values(price_m2_col, ascending=True)
        fig_m2 = px.bar(
            src_m2, x=price_m2_col, y="property_type",
            orientation="h",
            color="property_type",
            color_discrete_map=COLORS,
            labels={price_m2_col: "Giá TB/m² (triệu VND)", "property_type": ""},
        )
        fig_m2.update_layout(showlegend=False)
        st.plotly_chart(fig_m2, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu giá/m².")

st.divider()

# ── Row 3: Phân cụm K-Means & Pháp lý ────────────────────────
row3_c1, row3_c2 = st.columns(2)

with row3_c1:
    st.subheader("Phân khúc thị trường (K-Means)")
    st.caption("5 phân khúc từ bình dân đến siêu cao cấp")

    if not df_cluster.empty:
        seg_col  = "segment" if "segment" in df_cluster.columns else df_cluster.columns[0]
        price_col = "avg_price" if "avg_price" in df_cluster.columns else "avg_price_billion"
        count_col = "count" if "count" in df_cluster.columns else "total_listings"

        fig_seg = px.scatter(
            df_cluster,
            x=price_col, y="avg_area" if "avg_area" in df_cluster.columns else price_col,
            size=count_col, color=seg_col,
            labels={price_col: "Giá TB (tỷ)", "avg_area": "DT TB (m²)", seg_col: "Phân khúc"},
            size_max=50,
        )
        st.plotly_chart(fig_seg, use_container_width=True)
    else:
        if not df_filt.empty and "price_billion" in df_filt.columns and "area_m2" in df_filt.columns:
            fig_scatter = px.scatter(
                df_filt.sample(min(500, len(df_filt))),
                x="price_billion", y="area_m2",
                color="property_type" if "property_type" in df_filt.columns else None,
                color_discrete_map=COLORS,
                labels={"price_billion": "Giá (tỷ VND)", "area_m2": "Diện tích (m²)"},
                opacity=0.6,
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.info("Chưa có dữ liệu phân cụm. Chạy 02_kmeans.py trước.")

with row3_c2:
    st.subheader("Tình trạng Pháp lý")
    st.caption("Phân bổ tin đăng theo giấy tờ pháp lý")

    src_legal = df_legal if not df_legal.empty else (
        df_filt.groupby("legal_status").size().reset_index(name="count")
        if not df_filt.empty and "legal_status" in df_filt.columns else pd.DataFrame()
    )

    if not src_legal.empty and "legal_status" in src_legal.columns:
        src_legal = src_legal[src_legal["legal_status"].notna() & (src_legal["legal_status"] != "None")]
        if not src_legal.empty:
            fig_legal = px.pie(
                src_legal, names="legal_status", values="count",
                color_discrete_sequence=px.colors.qualitative.Set2,
                hole=0.3,
            )
            st.plotly_chart(fig_legal, use_container_width=True)
        else:
            st.info("Không có dữ liệu pháp lý.")
    else:
        st.info("Chưa có dữ liệu pháp lý.")

st.divider()

# ── Row 4: Hướng nhà & Real-time listings ────────────────────
row4_c1, row4_c2 = st.columns(2)

with row4_c1:
    st.subheader("Phân bổ Hướng Nhà")
    st.caption("Hướng nào được rao bán nhiều & giá cao nhất")

    src_dir = df_direction if not df_direction.empty else (
        df_filt.groupby("direction").agg(
            count=("listing_id", "count"),
            avg_price_per_m2=("price_per_m2_million", "mean"),
        ).reset_index()
        if not df_filt.empty and "direction" in df_filt.columns else pd.DataFrame()
    )

    if not src_dir.empty and "direction" in src_dir.columns:
        src_dir = src_dir[src_dir["direction"].notna() & (src_dir["direction"] != "None")]
        m2_col  = "avg_price_per_m2" if "avg_price_per_m2" in src_dir.columns else src_dir.columns[1]
        if not src_dir.empty:
            fig_dir = px.bar_polar(
                src_dir, r="count", theta="direction",
                color=m2_col,
                color_continuous_scale="Sunset",
                labels={"count": "Số tin", m2_col: "Giá/m² (tr)"},
            )
            st.plotly_chart(fig_dir, use_container_width=True)
        else:
            st.info("Không có dữ liệu hướng nhà.")
    else:
        st.info("Chưa có dữ liệu hướng nhà.")

with row4_c2:
    st.subheader("⚡ Tin mới nhất (Real-time)")
    st.caption("Dữ liệu đang stream từ Kafka → HDFS")

    if not df_stream.empty:
        cols_to_show = [c for c in ["title", "property_type", "price_raw", "district", "area_m2"] if c in df_stream.columns]
        latest = df_stream.sort_values("ingestion_time", ascending=False).head(8) if "ingestion_time" in df_stream.columns else df_stream.head(8)
        for _, row in latest.iterrows():
            with st.container(border=True):
                title   = row.get("title", "")[:60] + "..." if len(str(row.get("title", ""))) > 60 else row.get("title", "Không có tiêu đề")
                price   = row.get("price_raw", "—")
                dist    = row.get("district", "—")
                ptype   = row.get("property_type", "—")
                st.write(f"🏡 **{title}**")
                st.caption(f"{ptype} · {dist} · {price} · :green[**MỚI**]")
    else:
        st.info("Đang chờ dữ liệu streaming từ Kafka...")
        st.caption("Chạy: `docker exec spark-master python /spark-jobs/03_kafka_producer.py`")

st.divider()

# ── Bảng dữ liệu chi tiết ─────────────────────────────────────
with st.expander("Xem dữ liệu chi tiết (Top 100 tin đăng)", expanded=False):
    if not df_filt.empty:
        display_cols = [c for c in [
            "listing_id", "title", "property_type", "price_billion",
            "area_m2", "price_per_m2_million", "district", "city",
            "bedrooms", "legal_status", "price_tier",
        ] if c in df_filt.columns]
        st.dataframe(df_filt[display_cols].head(100), use_container_width=True)
    else:
        st.info("Không có dữ liệu.")

st.caption("Lambda Architecture · batdongsan.com · HDFS + Spark + Kafka + Hive + Streamlit")
