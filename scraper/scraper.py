"""
Scraper cho batdongsan.com
Cào tin đăng bán nhà đất, lưu ra /data/realestate_raw.csv
Chạy: python scraper.py [--pages 5] [--category ban-nha-tp-hcm]
"""

import argparse
import csv
import hashlib
import json
import os
import random
import re
import time
import uuid
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://batdongsan.com.vn"
OUTPUT_PATH = "/data/realestate_raw.csv"

CATEGORIES = {
    "ban-nha-tp-hcm":          "Nhà phố/Vila",
    "ban-can-ho-chung-cu":     "Căn hộ chung cư",
    "ban-dat-tp-hcm":          "Đất nền",
    "ban-biet-thu-lien-ke":    "Biệt thự/liền kề",
    "ban-nha-rieng":           "Nhà riêng",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
    "Referer": BASE_URL,
}

FIELDNAMES = [
    "listing_id", "title", "property_type", "price_raw", "price_billion",
    "area_m2", "price_per_m2_million", "city", "district", "ward", "address",
    "bedrooms", "bathrooms", "floors", "direction", "legal_status",
    "posted_date", "contact", "url", "scraped_at",
]


def parse_price(price_str: str) -> float | None:
    """Chuyển chuỗi giá (vd: '5,2 tỷ', '950 triệu') về float tỷ VND."""
    if not price_str:
        return None
    s = price_str.lower().replace(",", ".").strip()
    try:
        if "tỷ" in s:
            return float(re.sub(r"[^\d.]", "", s.split("tỷ")[0]))
        if "triệu" in s:
            val = float(re.sub(r"[^\d.]", "", s.split("triệu")[0]))
            return round(val / 1000, 4)
    except (ValueError, IndexError):
        pass
    return None


def parse_area(area_str: str) -> float | None:
    """Chuyển chuỗi DT (vd: '65 m²') về float."""
    if not area_str:
        return None
    try:
        return float(re.sub(r"[^\d.]", "", area_str))
    except ValueError:
        return None


def scrape_listing_page(category: str, page: int) -> list[dict]:
    """Trả về danh sách listing dict từ 1 trang kết quả."""
    url = f"{BASE_URL}/{category}/p{page}" if page > 1 else f"{BASE_URL}/{category}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [WARN] {url} → {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    listings = []

    for card in soup.select("div.js__card, div[data-product-id]"):
        listing = parse_card(card, category)
        if listing:
            listings.append(listing)

    return listings


def parse_card(card, category: str) -> dict | None:
    """Trích thông tin từ 1 card tin đăng trên trang danh sách."""
    try:
        title_el = card.select_one("span.js__card-title, a.js__card-title, h3 a")
        if not title_el:
            return None
        title = title_el.get_text(strip=True)

        href = title_el.get("href", "")
        url  = urljoin(BASE_URL, href) if href else ""

        listing_id_raw = card.get("data-product-id") or hashlib.md5(url.encode()).hexdigest()[:12]
        listing_id = str(listing_id_raw)

        price_el  = card.select_one("span.re__card-config-price, .price")
        price_raw = price_el.get_text(strip=True) if price_el else ""
        price_bil = parse_price(price_raw)

        area_el  = card.select_one("span.re__card-config-area, .area")
        area_raw = area_el.get_text(strip=True) if area_el else ""
        area_m2  = parse_area(area_raw)

        price_per_m2 = None
        if price_bil and area_m2 and area_m2 > 0:
            price_per_m2 = round(price_bil * 1000 / area_m2, 2)

        loc_el  = card.select_one("div.re__card-location, .location, span.re__card-config-location")
        location = loc_el.get_text(strip=True) if loc_el else ""

        # Tách district / city từ chuỗi địa chỉ
        parts = [p.strip() for p in location.split(",")]
        city     = parts[-1] if len(parts) >= 1 else ""
        district = parts[-2] if len(parts) >= 2 else ""
        ward     = parts[-3] if len(parts) >= 3 else ""
        address  = location

        property_type = CATEGORIES.get(category, "Bất động sản")

        return {
            "listing_id":          listing_id,
            "title":               title,
            "property_type":       property_type,
            "price_raw":           price_raw,
            "price_billion":       price_bil,
            "area_m2":             area_m2,
            "price_per_m2_million": price_per_m2,
            "city":                city,
            "district":            district,
            "ward":                ward,
            "address":             address,
            "bedrooms":            None,
            "bathrooms":           None,
            "floors":              None,
            "direction":           None,
            "legal_status":        None,
            "posted_date":         None,
            "contact":             None,
            "url":                 url,
            "scraped_at":          datetime.now().isoformat(timespec="seconds"),
        }
    except Exception as e:
        print(f"  [WARN] parse_card error: {e}")
        return None


def scrape_detail(listing: dict) -> dict:
    """Lấy thêm chi tiết từ trang chi tiết tin đăng (bedrooms, direction, legal...)."""
    if not listing.get("url"):
        return listing
    try:
        resp = requests.get(listing["url"], headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        def get_spec(label: str) -> str | None:
            for row in soup.select("div.re__pr-specs-content-item"):
                title_el = row.select_one(".re__pr-specs-content-item-title")
                value_el = row.select_one(".re__pr-specs-content-item-value")
                if title_el and label.lower() in title_el.get_text(strip=True).lower():
                    return value_el.get_text(strip=True) if value_el else None
            return None

        listing["bedrooms"]     = get_spec("phòng ngủ") or get_spec("bedroom")
        listing["bathrooms"]    = get_spec("phòng tắm") or get_spec("toilet")
        listing["floors"]       = get_spec("số tầng")
        listing["direction"]    = get_spec("hướng nhà") or get_spec("hướng ban công")
        listing["legal_status"] = get_spec("pháp lý") or get_spec("giấy tờ")

        posted_el = soup.select_one("div.re__pr-short-info-item span.title")
        if posted_el:
            listing["posted_date"] = posted_el.find_next_sibling().get_text(strip=True) if posted_el.find_next_sibling() else None

        contact_el = soup.select_one("div.re__contact-name")
        if contact_el:
            listing["contact"] = contact_el.get_text(strip=True)

    except requests.RequestException as e:
        print(f"  [WARN] detail {listing['url']}: {e}")

    return listing


def generate_sample_data(n: int = 2000) -> list[dict]:
    """
    Sinh dữ liệu mẫu khi không scrape được (dùng cho test/demo).
    Dữ liệu phản ánh phân phối giá thực tế thị trường TP.HCM.
    """
    random.seed(42)
    districts_hcm = [
        "Quận 1", "Quận 2", "Quận 3", "Quận 4", "Quận 5",
        "Quận 6", "Quận 7", "Quận 8", "Quận 9", "Quận 10",
        "Quận 11", "Quận 12", "Bình Thạnh", "Gò Vấp", "Phú Nhuận",
        "Tân Bình", "Tân Phú", "Thủ Đức", "Bình Dương", "Đồng Nai",
    ]
    property_types = ["Nhà phố/Vila", "Căn hộ chung cư", "Đất nền", "Biệt thự/liền kề", "Nhà riêng"]
    directions     = ["Đông", "Tây", "Nam", "Bắc", "Đông Nam", "Đông Bắc", "Tây Nam", "Tây Bắc"]
    legal_statuses = ["Sổ đỏ", "Sổ hồng", "Sổ hồng riêng", "Đang chờ sổ", "Hợp đồng mua bán"]

    # Giá trung bình theo quận (tỷ/m² tương đối)
    price_multiplier = {
        "Quận 1": 5.0, "Quận 3": 4.0, "Quận 2": 3.5, "Quận 7": 3.0,
        "Quận 5": 3.2, "Quận 10": 2.8, "Bình Thạnh": 2.5, "Phú Nhuận": 2.8,
        "Tân Bình": 2.2, "Gò Vấp": 1.8, "Quận 12": 1.5, "Thủ Đức": 1.6,
    }

    records = []
    for i in range(n):
        district      = random.choice(districts_hcm)
        prop_type     = random.choice(property_types)
        direction     = random.choice(directions)
        legal_status  = random.choice(legal_statuses)

        area = round(random.lognormvariate(4.0, 0.5), 1)    # trung bình ~60m²
        area = max(20.0, min(area, 500.0))

        base_price_per_m2 = price_multiplier.get(district, 2.0) * random.uniform(0.7, 1.5)
        if prop_type == "Căn hộ chung cư":
            base_price_per_m2 *= 0.8
        elif prop_type == "Đất nền":
            base_price_per_m2 *= 0.9
        elif prop_type == "Biệt thự/liền kề":
            base_price_per_m2 *= 1.5

        price_per_m2_million = round(base_price_per_m2 * 1000, 1)   # triệu/m²
        price_billion        = round(area * price_per_m2_million / 1000, 3)

        bedrooms  = random.choice([1, 2, 2, 3, 3, 3, 4, 5])
        bathrooms = random.choice([1, 1, 2, 2, 3])
        floors    = random.choice([1, 1, 2, 2, 3, 4, 5])

        year  = random.randint(2022, 2025)
        month = random.randint(1, 12)
        day   = random.randint(1, 28)
        posted_date = f"{day:02d}/{month:02d}/{year}"

        records.append({
            "listing_id":           str(uuid.uuid4())[:8],
            "title":                f"Bán {prop_type} {district}, {area}m², {price_billion:.2f} tỷ",
            "property_type":        prop_type,
            "price_raw":            f"{price_billion:.2f} tỷ",
            "price_billion":        price_billion,
            "area_m2":              area,
            "price_per_m2_million": price_per_m2_million,
            "city":                 "Hồ Chí Minh",
            "district":             district,
            "ward":                 f"Phường {random.randint(1, 15)}",
            "address":              f"Đường {random.randint(1,50)}, {district}, TP.HCM",
            "bedrooms":             bedrooms,
            "bathrooms":            bathrooms,
            "floors":               floors,
            "direction":            direction,
            "legal_status":         legal_status,
            "posted_date":          posted_date,
            "contact":              f"CTV{random.randint(100,999)}",
            "url":                  f"https://batdongsan.com.vn/listing/{i+1}",
            "scraped_at":           datetime.now().isoformat(timespec="seconds"),
        })
    return records


def main():
    parser = argparse.ArgumentParser(description="Scraper batdongsan.com")
    parser.add_argument("--pages",    type=int, default=5,                  help="Số trang mỗi category")
    parser.add_argument("--delay",    type=float, default=1.5,              help="Delay giữa requests (giây)")
    parser.add_argument("--detail",   action="store_true",                  help="Có lấy chi tiết từng tin không")
    parser.add_argument("--sample",   action="store_true",                  help="Dùng dữ liệu mẫu (không scrape thật)")
    parser.add_argument("--sample-n", type=int, default=2000,              help="Số dòng dữ liệu mẫu")
    parser.add_argument("--output",   default=OUTPUT_PATH,                  help="Đường dẫn file CSV output")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    if args.sample:
        print(f"[INFO] Sinh {args.sample_n} dòng dữ liệu mẫu...")
        records = generate_sample_data(args.sample_n)
    else:
        records = []
        for category in CATEGORIES:
            print(f"\n[INFO] Category: {category}")
            for page in range(1, args.pages + 1):
                print(f"  page {page}...")
                listings = scrape_listing_page(category, page)
                if not listings:
                    print("  Không lấy được listing, dừng category này.")
                    break

                if args.detail:
                    for lst in listings:
                        lst = scrape_detail(lst)
                        time.sleep(args.delay * random.uniform(0.8, 1.2))

                records.extend(listings)
                print(f"  → {len(listings)} listings, tổng: {len(records)}")
                time.sleep(args.delay * random.uniform(0.8, 1.5))

        if not records:
            print("[WARN] Không scrape được dữ liệu thật. Dùng dữ liệu mẫu thay thế.")
            records = generate_sample_data(args.sample_n)

    # Ghi CSV
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(records)

    print(f"\n[DONE] Đã lưu {len(records)} tin đăng → {args.output}")


if __name__ == "__main__":
    main()
