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

COLORS = {
    "Nhà phố/Vila":      "#1db954",
    "Căn hộ chung cư":   "#ff6b6b",
    "Đất nền":           "#ffd700",
    "Biệt thự/liền kề": "#a78bfa",
    "Nhà riêng":         "#38bdf8",
}

TIER_ORDER = ["Dưới 2 tỷ", "2-5 tỷ", "5-10 tỷ", "10-20 tỷ", "Trên 20 tỷ"]

# ── Load helpers ───────────────────────────────────────────────
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
df_price_dist = load_batch("price_dist.parquet")
df_cluster    = load_batch("cluster_stats.parquet")
df_legal      = load_batch("by_legal.parquet")
df_direction  = load_batch("by_direction.parquet")

# Batch views (fallback nếu chưa có merged)
df_batch_dist = load_batch("by_district.parquet")
df_batch_type = load_batch("by_type.parquet")

# Speed views
df_speed_dist = load_batch("speed_by_district.parquet")
df_speed_type = load_batch("speed_by_type.parquet")
df_speed_summary = load_batch("speed_summary.parquet")

# Merged views (Serving Layer = Batch + Speed)
df_merged_dist = load_batch("merged_by_district.parquet")
df_merged_type = load_batch("merged_by_type.parquet")

# Ưu tiên merged, fallback về batch
df_district = df_merged_dist if not df_merged_dist.empty else df_batch_dist
df_type     = df_merged_type if not df_merged_type.empty else df_batch_type
is_merged   = not df_merged_dist.empty

# Linear Regression
df_lr_metrics = load_batch("lr_metrics.parquet")
df_lr_coef    = load_batch("lr_coefficients.parquet")
df_lr_preds   = load_batch("lr_predictions.parquet")
df_lr_catmap  = load_batch("lr_category_map.parquet")
df_lr_params  = load_batch("lr_model_params.parquet")

# Real-time stream (WebHDFS)
df_stream = load_streaming()

# ── Lấy thông tin speed layer ─────────────────────────────────
speed_total = 0
batch_total = 0
latest_ingestion = "N/A"
if not df_speed_summary.empty:
    r = df_speed_summary.iloc[0]
    speed_total      = int(r.get("stream_total", 0))
    batch_total      = int(r.get("batch_total",  len(df_cleaned)))
    latest_ingestion = str(r.get("latest_ingestion", "N/A"))
elif not df_cleaned.empty:
    batch_total = len(df_cleaned)

# ── Sidebar ────────────────────────────────────────────────────
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

    # ── Lambda Architecture Status ────────────────────────────
    st.subheader("⚡ Lambda Layer Status")

    batch_ok  = not df_cleaned.empty
    speed_ok  = speed_total > 0
    serving_ok = is_merged

    st.write(f"{'✅' if batch_ok  else '❌'} **Batch Layer**")
    st.caption(f"  {batch_total:,} records · HDFS + Hive")

    st.write(f"{'✅' if speed_ok  else '🟡'} **Speed Layer**")
    st.caption(f"  {speed_total:,} stream records · Kafka → Spark")

    st.write(f"{'✅' if serving_ok else '🟡'} **Serving Layer**")
    if serving_ok:
        merged_total_sidebar = batch_total + speed_total
        st.caption(f"  Merged {merged_total_sidebar:,} · Batch + Speed")
    else:
        st.caption("  Batch-only (chưa có stream)")

    st.divider()
    st.caption("Dữ liệu từ batdongsan.com")


# ── Filter trên df_cleaned ────────────────────────────────────
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
    st.caption("Lambda Architecture · batdongsan.com · HDFS + Spark + Kafka + Hive + Streamlit")
with col_h2:
    if speed_ok:
        st.success("🟢 STREAM ACTIVE")
    elif not df_stream.empty:
        st.success("🟢 LIVE FEED")
    else:
        st.warning("🟡 Batch-only")

st.divider()

# ── KPI Cards ─────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)

# Tổng tin = batch + speed (Serving Layer merge)
merged_count = (len(df_filt) if not df_filt.empty else 0) + speed_total
k1.metric(
    "Tổng tin (Batch + Speed)",
    f"{merged_count:,}",
    delta=f"+{speed_total:,} stream" if speed_total > 0 else None,
)

if not df_filt.empty and "price_billion" in df_filt.columns:
    k2.metric("Giá TB (Batch)", f"{df_filt['price_billion'].mean():.2f} tỷ")
else:
    k2.metric("Giá TB (Batch)", "—")

if not df_filt.empty and "price_per_m2_million" in df_filt.columns:
    k3.metric("Giá TB/m²", f"{df_filt['price_per_m2_million'].mean():.0f} tr/m²")
else:
    k3.metric("Giá TB/m²", "—")

if not df_filt.empty and "district" in df_filt.columns:
    k4.metric("Quận nhiều nhất", df_filt["district"].value_counts().idxmax())
else:
    k4.metric("Quận nhiều nhất", "—")

k5.metric(
    "Real-time mới nhất",
    f"+{len(df_stream):,}" if not df_stream.empty else f"+{speed_total:,}",
)

st.write("")

# ── Row 1: Giá theo quận & Phân bổ loại BĐS ──────────────────
row1_c1, row1_c2 = st.columns(2)

with row1_c1:
    label = "Giá TB theo Quận/Huyện" + (" ✦ Merged" if is_merged else " (Batch)")
    st.subheader(label)
    st.caption("Top 15 quận · " + ("Batch + Speed Layer" if is_merged else "Batch Layer only"))

    src = df_district
    if src.empty and not df_filt.empty and "district" in df_filt.columns:
        src = df_filt.groupby("district").agg(
            avg_price_billion=("price_billion", "mean"),
            merged_listings=("listing_id", "count"),
        ).reset_index()

    if not src.empty:
        price_col   = next((c for c in ["merged_avg_price","avg_price_billion","avg_price"] if c in src.columns), None)
        count_col   = next((c for c in ["merged_listings","total_listings","count"] if c in src.columns), None)
        if price_col and count_col:
            src_top = src.dropna(subset=[price_col]).nlargest(15, count_col)
            fig = px.bar(
                src_top, x="district", y=price_col,
                color=price_col,
                color_continuous_scale="Viridis",
                labels={"district": "Quận/Huyện", price_col: "Giá TB (tỷ VND)"},
            )
            fig.update_layout(coloraxis_showscale=False, xaxis_tickangle=-35)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu quận.")

with row1_c2:
    label2 = "Phân bổ theo Loại BĐS" + (" ✦ Merged" if is_merged else " (Batch)")
    st.subheader(label2)
    st.caption("Số tin đăng · " + ("Batch + Speed Layer" if is_merged else "Batch Layer only"))

    src_type = df_type
    if src_type.empty and not df_filt.empty and "property_type" in df_filt.columns:
        src_type = df_filt.groupby("property_type").size().reset_index(name="merged_listings")

    if not src_type.empty:
        count_col = next((c for c in ["merged_listings","total_listings","count"] if c in src_type.columns), None)
        if count_col:
            fig_pie = px.pie(
                src_type, names="property_type", values=count_col,
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
    st.caption("Tỷ lệ tin đăng theo phân khúc giá · Batch Layer")

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
    st.subheader("Giá/m² TB theo Loại BĐS")
    st.caption("So sánh hiệu quả đầu tư · " + ("Merged" if is_merged else "Batch"))

    src_m2 = df_type if not df_type.empty else (
        df_filt.groupby("property_type")["price_per_m2_million"].mean().reset_index()
        if not df_filt.empty and "property_type" in df_filt.columns else pd.DataFrame()
    )

    if not src_m2.empty:
        m2_col = next((c for c in ["avg_price_per_m2","price_per_m2_million"] if c in src_m2.columns), None)
        if m2_col:
            src_m2 = src_m2.dropna(subset=[m2_col]).sort_values(m2_col, ascending=True)
            fig_m2 = px.bar(
                src_m2, x=m2_col, y="property_type",
                orientation="h",
                color="property_type",
                color_discrete_map=COLORS,
                labels={m2_col: "Giá TB/m² (triệu VND)", "property_type": ""},
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
    st.caption("5 phân khúc từ bình dân đến siêu cao cấp · Batch Layer")

    if not df_cluster.empty:
        seg_col   = "segment" if "segment" in df_cluster.columns else df_cluster.columns[0]
        price_col = next((c for c in ["avg_price","avg_price_billion"] if c in df_cluster.columns), None)
        count_col = next((c for c in ["count","total_listings"] if c in df_cluster.columns), None)
        if price_col and count_col:
            fig_seg = px.scatter(
                df_cluster,
                x=price_col,
                y="avg_area" if "avg_area" in df_cluster.columns else price_col,
                size=count_col, color=seg_col,
                labels={price_col: "Giá TB (tỷ)", "avg_area": "DT TB (m²)", seg_col: "Phân khúc"},
                size_max=50,
            )
            st.plotly_chart(fig_seg, use_container_width=True)
    elif not df_filt.empty and "price_billion" in df_filt.columns and "area_m2" in df_filt.columns:
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

# ── Row 4: Hướng nhà & Real-time ──────────────────────────────
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
        m2_col  = next((c for c in ["avg_price_per_m2","avg_price_m2"] if c in src_dir.columns), None)
        if not src_dir.empty and m2_col:
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
        latest = (
            df_stream.sort_values("ingestion_time", ascending=False).head(8)
            if "ingestion_time" in df_stream.columns else df_stream.head(8)
        )
        for _, row in latest.iterrows():
            title = str(row.get("title", ""))
            title = (title[:60] + "…") if len(title) > 60 else (title or "Không có tiêu đề")
            with st.container(border=True):
                st.write(f"🏡 **{title}**")
                st.caption(
                    f"{row.get('property_type','—')} · "
                    f"{row.get('district','—')} · "
                    f"{row.get('price_raw','—')} · :green[**MỚI**]"
                )
    else:
        st.info("Đang chờ dữ liệu streaming từ Kafka...")
        st.caption("Chạy: `docker exec spark-master python /spark-jobs/03_kafka_producer.py`")

st.divider()

# ══════════════════════════════════════════════════════════════
# LINEAR REGRESSION
# ══════════════════════════════════════════════════════════════
st.header("🤖 Dự đoán giá BĐS — Linear Regression")
st.caption("Mô hình huấn luyện trên Spark MLlib · Features: diện tích, phòng ngủ, phòng tắm, số tầng, loại BĐS, quận")

lr_ready = not df_lr_metrics.empty and not df_lr_coef.empty

if not lr_ready:
    st.info("Chưa có kết quả mô hình. Chạy: `python /spark-jobs/06_linear_regression.py` rồi `05_export.py`")
else:
    metrics_map = dict(zip(df_lr_metrics["metric"], df_lr_metrics["value"]))
    r2   = metrics_map.get("R²", 0)
    rmse = metrics_map.get("RMSE (tỷ)", 0)
    mae  = metrics_map.get("MAE (tỷ)", 0)
    n_train = int(metrics_map.get("Train rows", 0))

    lm1, lm2, lm3, lm4 = st.columns(4)
    lm1.metric("R² Score",      f"{r2:.4f}",   help="Gần 1 càng tốt")
    lm2.metric("RMSE",          f"{rmse:.2f} tỷ", help="Sai số bình phương trung bình")
    lm3.metric("MAE",           f"{mae:.2f} tỷ",  help="Sai số tuyệt đối trung bình")
    lm4.metric("Train samples", f"{n_train:,}")

    st.write("")
    lr_c1, lr_c2 = st.columns(2)

    with lr_c1:
        st.subheader("Trọng số đặc trưng (Coefficients)")
        st.caption("Tác động của mỗi feature lên giá dự đoán (tỷ VND)")

        FEAT_LABELS = {
            "area_m2":           "Diện tích (m²)",
            "bedrooms":          "Số phòng ngủ",
            "bathrooms":         "Số phòng tắm",
            "floors":            "Số tầng",
            "property_type_idx": "Loại BĐS (index)",
            "district_idx":      "Quận/Huyện (index)",
        }
        coef_sorted = df_lr_coef.sort_values("coefficient")
        coef_sorted["label"] = coef_sorted["feature"].map(lambda x: FEAT_LABELS.get(x, x))
        bar_colors = ["#e74c3c" if v < 0 else "#2ecc71" for v in coef_sorted["coefficient"]]

        fig_coef = go.Figure(go.Bar(
            x=coef_sorted["coefficient"],
            y=coef_sorted["label"],
            orientation="h",
            marker_color=bar_colors,
            text=[f"{v:+.3f}" for v in coef_sorted["coefficient"]],
            textposition="outside",
        ))
        fig_coef.update_layout(
            xaxis_title="Coefficient (tỷ VND)",
            margin=dict(l=10, r=70, t=10, b=10),
            height=300,
        )
        st.plotly_chart(fig_coef, use_container_width=True)

    with lr_c2:
        st.subheader("Thực tế vs Dự đoán")
        st.caption("Điểm càng gần đường chéo đỏ → mô hình càng chính xác")

        if not df_lr_preds.empty:
            p95 = df_lr_preds["actual"].quantile(0.95)
            df_sc = df_lr_preds[
                (df_lr_preds["actual"] <= p95) &
                (df_lr_preds["predicted"] <= p95 * 1.5)
            ]
            fig_sc = px.scatter(
                df_sc,
                x="actual", y="predicted",
                color="property_type" if "property_type" in df_sc.columns else None,
                color_discrete_map=COLORS,
                labels={"actual": "Giá thực tế (tỷ)", "predicted": "Giá dự đoán (tỷ)"},
                opacity=0.55,
            )
            max_val = float(df_sc[["actual", "predicted"]].max().max())
            fig_sc.add_shape(
                type="line", x0=0, y0=0, x1=max_val, y1=max_val,
                line=dict(color="red", dash="dash", width=1.5),
            )
            fig_sc.update_layout(height=300, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig_sc, use_container_width=True)
        else:
            st.info("Chưa có dữ liệu dự đoán.")

    st.divider()

    # ── Prediction form ────────────────────────────────────────
    st.subheader("🔮 Thử dự đoán giá BĐS mới")
    st.caption("Nhập thông số → mô hình tính giá ước tính")

    type_opts = []
    dist_opts = []
    if not df_lr_catmap.empty:
        type_opts = df_lr_catmap[df_lr_catmap["feature"] == "property_type"]["label"].tolist()
        dist_opts = df_lr_catmap[df_lr_catmap["feature"] == "district"]["label"].tolist()

    pred_c1, pred_c2 = st.columns(2)
    with pred_c1:
        area   = st.number_input("Diện tích (m²)",  min_value=10.0, max_value=1000.0, value=60.0, step=5.0)
        beds   = st.number_input("Số phòng ngủ",     min_value=1,    max_value=20,     value=2,    step=1)
        baths  = st.number_input("Số phòng tắm",     min_value=1,    max_value=20,     value=2,    step=1)
    with pred_c2:
        floors = st.number_input("Số tầng",          min_value=1,    max_value=50,     value=2,    step=1)
        ptype  = st.selectbox("Loại BĐS",   type_opts if type_opts else ["Nhà phố/Vila"])
        dist   = st.selectbox("Quận/Huyện", dist_opts if dist_opts else ["Quận 1"])

    if st.button("Dự đoán giá", type="primary", use_container_width=True):
        if df_lr_params.empty or df_lr_catmap.empty:
            st.error("Chưa có model params. Chạy 06_linear_regression.py trước.")
        else:
            p = df_lr_params.iloc[0]

            def get_idx(feature, label):
                row = df_lr_catmap[
                    (df_lr_catmap["feature"] == feature) &
                    (df_lr_catmap["label"]   == label)
                ]
                return float(row["index"].values[0]) if len(row) > 0 else 0.0

            predicted = max(0.0,
                p["intercept"]
                + p["coef_area_m2"]       * area
                + p["coef_bedrooms"]      * beds
                + p["coef_bathrooms"]     * baths
                + p["coef_floors"]        * floors
                + p["coef_property_type"] * get_idx("property_type", ptype)
                + p["coef_district"]      * get_idx("district", dist)
            )
            price_per_m2 = (predicted / area * 1000) if area > 0 else 0

            r1, r2c, r3 = st.columns(3)
            r1.metric("Giá dự đoán",  f"{predicted:.2f} tỷ VND")
            r2c.metric("Giá/m²",      f"{price_per_m2:.0f} triệu/m²")
            r3.metric("Loại BĐS",     ptype)
            st.caption(
                f"⚠️ Tham khảo — R²={r2:.4f} · Sai số trung bình ±{mae:.2f} tỷ VND"
            )

st.divider()

# ── Lambda Architecture flow diagram ──────────────────────────
with st.expander("📐 Lambda Architecture — Sơ đồ luồng dữ liệu", expanded=False):
    st.markdown("""
    ```
    ┌─────────────────────────────────────────────────────────────────┐
    │                    DATA SOURCES                                  │
    │              batdongsan.com (scraper)                           │
    └──────────────────────┬──────────────────────────────────────────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
    ┌─────────────────┐       ┌──────────────────────┐
    │   BATCH LAYER   │       │     SPEED LAYER       │
    │                 │       │                        │
    │  00_load_csv    │       │  Kafka Topic           │
    │  01_eda         │       │  realestate-stream     │
    │  02_kmeans      │       │       │                │
    │  06_linreg  ────┼──┐    │  03_streaming ─────── ┼──┐
    │                 │  │    │  (Spark Structured)    │  │
    │  → HDFS /batch  │  │    │  → HDFS /streaming     │  │
    │  → Hive tables  │  │    │                        │  │
    └─────────────────┘  │    └────────────────────────┘  │
                         │                                 │
                         └──────────────┬──────────────────┘
                                        ▼
                          ┌─────────────────────────┐
                          │     SERVING LAYER        │
                          │                          │
                          │  05_export               │
                          │  · Speed views (agg)     │
                          │  · Merge batch + speed   │
                          │  → /data/*.parquet        │
                          │                          │
                          │  04_hive_query           │
                          │  · merged_listings VIEW  │
                          │  · merged_district_stats │
                          └────────────┬─────────────┘
                                       │
                                       ▼
                          ┌─────────────────────────┐
                          │   STREAMLIT DASHBOARD    │
                          │   (Serving Layer UI)     │
                          │                          │
                          │  · Batch + Speed merge   │
                          │  · KPI cards             │
                          │  · Linear Regression     │
                          │  · Real-time feed        │
                          └─────────────────────────┘
    ```
    """)

# ── Dữ liệu chi tiết ──────────────────────────────────────────
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

st.caption(
    f"Lambda Architecture · HDFS + Spark + Kafka + Hive + Streamlit · "
    f"Batch {batch_total:,} | Speed {speed_total:,} | "
    f"Merged {batch_total + speed_total:,}"
)
