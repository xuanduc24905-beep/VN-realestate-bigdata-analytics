"""
00_load_csv.py — Đọc realestate_raw.csv, chuẩn hóa schema, lưu cleaned CSV.
"""
import pandas as pd
import numpy as np

INPUT_PATH  = "/data/realestate_raw.csv"
OUTPUT_PATH = "/data/realestate_cleaned.csv"

print(f"Reading {INPUT_PATH} ...")
df = pd.read_csv(INPUT_PATH, low_memory=False)
print(f"  Raw rows: {len(df)}, columns: {list(df.columns)}")

# Drop unnamed index
df = df.drop(columns=[c for c in df.columns if c.startswith("Unnamed")], errors="ignore")

# Ép kiểu số
num_cols = ["price_billion", "area_m2", "price_per_m2_million", "bedrooms", "bathrooms", "floors"]
for col in num_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Tính price_per_m2 nếu còn thiếu
mask = df["price_per_m2_million"].isna() & df["price_billion"].notna() & df["area_m2"].notna() & (df["area_m2"] > 0)
df.loc[mask, "price_per_m2_million"] = (df.loc[mask, "price_billion"] * 1000 / df.loc[mask, "area_m2"]).round(2)

# Phân loại giá
def price_tier(p):
    if pd.isna(p):   return None
    if p < 2:        return "Dưới 2 tỷ"
    if p < 5:        return "2-5 tỷ"
    if p < 10:       return "5-10 tỷ"
    if p < 20:       return "10-20 tỷ"
    return "Trên 20 tỷ"

df["price_tier"] = df["price_billion"].apply(price_tier)

# Phân loại diện tích
def area_tier(a):
    if pd.isna(a):   return None
    if a < 40:       return "Dưới 40m²"
    if a < 80:       return "40-80m²"
    if a < 150:      return "80-150m²"
    if a < 300:      return "150-300m²"
    return "Trên 300m²"

df["area_tier"] = df["area_m2"].apply(area_tier)

# Chuẩn hóa chuỗi
for col in ["city", "district", "ward", "property_type", "direction", "legal_status"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace("nan", None)

# Drop dòng thiếu dữ liệu chính
df = df.dropna(subset=["listing_id", "title", "price_billion"])
df = df.drop_duplicates(subset=["listing_id"])

# Lọc outlier giá (< 0.1 tỷ hoặc > 500 tỷ)
df = df[(df["price_billion"] >= 0.1) & (df["price_billion"] <= 500)]

print(f"  After cleaning: {len(df)} rows")
print(f"  Price range: {df['price_billion'].min():.2f} – {df['price_billion'].max():.2f} tỷ")
print(f"  Districts: {df['district'].nunique()} quận/huyện")

df.to_csv(OUTPUT_PATH, index=False)
print(f"\nSaved {len(df)} listings → {OUTPUT_PATH}")
print("Columns:", list(df.columns))
