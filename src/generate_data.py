"""Realistic synthetic e-commerce data generator.

Produces the seven source systems used by the platform:

    customers, products, orders, order_items, payments, shipments, returns

Design goals
------------
* 100k+ historical orders with a believable growth trend, weekly seasonality
  and holiday spikes, so the analytics layer shows real signal.
* Referential integrity across every entity (no orphan facts).
* Repeat-buyer behaviour via a Pareto-weighted customer sampler, which makes
  retention / RFM / CLV analysis meaningful instead of uniform noise.
* Deterministic output (seeded) so pipeline tests are reproducible.

Output: CSV for every source, plus JSON for two sources (to exercise the
multi-format ingestion path) and a manifest with row counts.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from .config import settings

FIRST_NAMES = [
    "Aarav", "Aditi", "Aisha", "Ananya", "Arjun", "Carlos", "Chloe", "Daniel",
    "Diya", "Elena", "Ethan", "Fatima", "Grace", "Hana", "Ishaan", "Ivan",
    "James", "Jia", "Kavya", "Liam", "Lucas", "Maya", "Meera", "Noah",
    "Olivia", "Priya", "Rahul", "Riya", "Rohan", "Sara", "Sofia", "Tara",
    "Vikram", "Wei", "Yuki", "Zara",
]
LAST_NAMES = [
    "Agarwal", "Bennett", "Chen", "Das", "Dubois", "Fernandez", "Garcia",
    "Gupta", "Hassan", "Iyer", "Johnson", "Kapoor", "Khan", "Kim", "Kumar",
    "Lee", "Mehta", "Miller", "Nair", "Nguyen", "Patel", "Rao", "Reddy",
    "Rossi", "Sharma", "Singh", "Smith", "Suzuki", "Taylor", "Verma",
    "Wang", "Williams", "Yadav", "Zhang",
]

# city, state, region
LOCATIONS = [
    ("New York", "NY", "Northeast"), ("Boston", "MA", "Northeast"),
    ("Philadelphia", "PA", "Northeast"), ("Newark", "NJ", "Northeast"),
    ("Chicago", "IL", "Midwest"), ("Detroit", "MI", "Midwest"),
    ("Columbus", "OH", "Midwest"), ("Minneapolis", "MN", "Midwest"),
    ("Atlanta", "GA", "South"), ("Miami", "FL", "South"),
    ("Dallas", "TX", "South"), ("Houston", "TX", "South"),
    ("Nashville", "TN", "South"), ("Charlotte", "NC", "South"),
    ("Phoenix", "AZ", "West"), ("Denver", "CO", "West"),
    ("Seattle", "WA", "West"), ("San Francisco", "CA", "West"),
    ("Los Angeles", "CA", "West"), ("San Diego", "CA", "West"),
    ("Austin", "TX", "South"), ("Portland", "OR", "West"),
    ("Las Vegas", "NV", "West"), ("Salt Lake City", "UT", "West"),
    ("Kansas City", "MO", "Midwest"), ("St. Louis", "MO", "Midwest"),
    ("Baltimore", "MD", "Northeast"), ("Richmond", "VA", "South"),
    ("Orlando", "FL", "South"), ("Sacramento", "CA", "West"),
]

CATEGORIES = {
    "Electronics": {
        "sub": ["Headphones", "Smartphones", "Laptops", "Cameras", "Smart Home"],
        "price": (29, 1299), "cost_ratio": (0.55, 0.78),
    },
    "Home & Kitchen": {
        "sub": ["Cookware", "Furniture", "Bedding", "Appliances", "Decor"],
        "price": (12, 599), "cost_ratio": (0.45, 0.68),
    },
    "Fashion": {
        "sub": ["Men's Wear", "Women's Wear", "Footwear", "Accessories"],
        "price": (9, 249), "cost_ratio": (0.32, 0.55),
    },
    "Beauty": {
        "sub": ["Skincare", "Haircare", "Makeup", "Fragrance"],
        "price": (6, 149), "cost_ratio": (0.28, 0.48),
    },
    "Sports & Outdoors": {
        "sub": ["Fitness", "Camping", "Cycling", "Team Sports"],
        "price": (14, 899), "cost_ratio": (0.48, 0.70),
    },
    "Books": {
        "sub": ["Fiction", "Non-Fiction", "Children", "Academic"],
        "price": (5, 79), "cost_ratio": (0.40, 0.60),
    },
    "Grocery": {
        "sub": ["Snacks", "Beverages", "Organic", "Pantry"],
        "price": (2, 89), "cost_ratio": (0.62, 0.82),
    },
    "Toys & Games": {
        "sub": ["Board Games", "Educational", "Outdoor Play", "Collectibles"],
        "price": (8, 299), "cost_ratio": (0.42, 0.65),
    },
    "Automotive": {
        "sub": ["Car Care", "Tools", "Interior", "Electronics"],
        "price": (7, 499), "cost_ratio": (0.50, 0.72),
    },
    "Health": {
        "sub": ["Supplements", "Medical", "Wellness", "Personal Care"],
        "price": (5, 199), "cost_ratio": (0.35, 0.58),
    },
}

BRANDS = ["Northwind", "Aurelia", "Vertex", "Lumina", "Kestrel", "Orbit",
          "Sable", "Terra", "Nimbus", "Zenith"]

CHANNELS = ["website", "mobile_app", "marketplace", "phone"]
CHANNEL_P = [0.42, 0.34, 0.20, 0.04]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "upi", "net_banking", "gift_card", "cod"]
PAYMENT_P = [0.34, 0.22, 0.18, 0.12, 0.07, 0.04, 0.03]
CARRIERS = ["UPS", "FedEx", "USPS", "DHL"]
RETURN_REASONS = ["damaged", "wrong_item", "not_as_described", "changed_mind",
                  "late_delivery", "quality_issue"]


def _weighted_days(rng: np.random.Generator, start: pd.Timestamp, end: pd.Timestamp) -> np.ndarray:
    """Daily order volume with growth trend + weekly + holiday seasonality."""
    days = pd.date_range(start, end, freq="D")
    t = np.arange(len(days)) / max(len(days) - 1, 1)

    growth = 0.55 + 1.45 * t ** 1.15          # steady ~3.6x growth over the window
    dow = days.dayofweek.to_numpy()
    weekly = np.where(dow >= 5, 1.22, 0.94)    # weekends convert better
    weekly = np.where(dow == 0, 0.88, weekly)  # Monday dip

    month = days.month.to_numpy()
    seasonal = np.ones(len(days))
    seasonal[np.isin(month, [11, 12])] = 1.55   # Black Friday / holiday
    seasonal[np.isin(month, [1])] = 0.82       # post-holiday lull
    seasonal[np.isin(month, [7])] = 1.08       # mid-year sale

    w = growth * weekly * seasonal
    return w / w.sum()


def generate(output_dir: Path | None = None) -> dict:
    out = Path(output_dir or settings.raw_dir)
    out.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(settings.random_seed)

    end = pd.Timestamp(settings.data_end_date) + pd.Timedelta(hours=23, minutes=59)
    start = end - pd.Timedelta(days=880)          # ~29 months of history

    # ------------------------------------------------------------------ customers
    n_c = settings.n_customers
    loc_idx = rng.integers(0, len(LOCATIONS), n_c)
    cities = np.array([LOCATIONS[i][0] for i in range(len(LOCATIONS))])[loc_idx]
    states = np.array([LOCATIONS[i][1] for i in range(len(LOCATIONS))])[loc_idx]
    regions = np.array([LOCATIONS[i][2] for i in range(len(LOCATIONS))])[loc_idx]

    signup_offsets = rng.integers(0, (end - pd.Timestamp("2023-01-01")).days, n_c)
    signup = pd.Timestamp("2023-01-01") + pd.to_timedelta(signup_offsets, unit="D")

    customers = pd.DataFrame({
        "customer_id": [f"C{i:06d}" for i in range(1, n_c + 1)],
        "first_name": rng.choice(FIRST_NAMES, n_c),
        "last_name": rng.choice(LAST_NAMES, n_c),
        "email": [f"customer{i:06d}@example.com" for i in range(1, n_c + 1)],
        "phone": [f"+1-{rng.integers(200, 989)}-{rng.integers(100, 999)}-{rng.integers(1000, 9999)}"
                  for _ in range(n_c)],
        "city": cities,
        "state": states,
        "region": regions,
        "country": "US",
        "postal_code": [f"{rng.integers(10000, 99999)}" for _ in range(n_c)],
        "signup_date": signup.strftime("%Y-%m-%d"),
        "loyalty_tier": rng.choice(["Bronze", "Silver", "Gold", "Platinum"], n_c,
                                   p=[0.55, 0.28, 0.13, 0.04]),
        "is_active": rng.choice([True, False], n_c, p=[0.93, 0.07]),
    })

    # ------------------------------------------------------------------- products
    n_p = settings.n_products
    cat_names = list(CATEGORIES)
    cat_pick = rng.choice(cat_names, n_p, p=[0.18, 0.14, 0.20, 0.10, 0.10, 0.08, 0.08, 0.06, 0.04, 0.02])
    rows = []
    for i in range(n_p):
        cat = cat_pick[i]
        sub = rng.choice(CATEGORIES[cat]["sub"])
        lo, hi = CATEGORIES[cat]["price"]
        price = round(float(np.exp(rng.uniform(np.log(lo), np.log(hi)))), 2)
        cr = CATEGORIES[cat]["cost_ratio"]
        cost = round(price * rng.uniform(*cr), 2)
        rows.append({
            "product_id": f"P{i + 1:04d}",
            "sku": f"{cat[:2].upper()}-{i + 1:05d}",
            "product_name": f"{rng.choice(BRANDS)} {sub} {i + 1}",
            "category": cat,
            "subcategory": sub,
            "brand": rng.choice(BRANDS),
            "unit_cost": cost,
            "unit_price": price,
            "weight_kg": round(float(rng.uniform(0.1, 22.0)), 3),
            "is_active": bool(rng.random() > 0.06),
            "launch_date": (pd.Timestamp("2022-06-01") +
                            pd.to_timedelta(int(rng.integers(0, 1200)), unit="D")).strftime("%Y-%m-%d"),
        })
    products = pd.DataFrame(rows)

    # --------------------------------------------------------------------- orders
    n_o = settings.n_orders
    day_weights = _weighted_days(rng, start, end)
    days = pd.date_range(start, end, freq="D")
    per_day = rng.multinomial(n_o, day_weights)
    order_days = np.repeat(np.arange(len(days)), per_day)

    # random time of day: retail traffic peaks late morning and evening
    hour_p = np.array([0.5, 0.3, 0.2, 0.2, 0.3, 0.6, 1.2, 2.0, 3.0, 3.6, 4.0, 4.2,
                       4.0, 3.9, 4.0, 4.1, 4.3, 4.6, 5.0, 5.2, 4.8, 3.6, 2.2, 1.2])
    hour_p = hour_p / hour_p.sum()
    secs = (rng.choice(24, n_o, p=hour_p) * 3600
            + rng.integers(0, 3600, n_o))
    order_ts = days[order_days] + pd.to_timedelta(secs, unit="s")
    order_ts = pd.DatetimeIndex(order_ts).sort_values()

    # repeat buyers: Pareto-like sampling over customers
    rank_w = 1.0 / np.power(np.arange(1, n_c + 1), 0.75)
    cust_p = rank_w / rank_w.sum()
    cust_idx = rng.choice(n_c, n_o, p=cust_p)

    status = rng.choice(["completed", "pending", "cancelled", "returned"],
                        n_o, p=[0.795, 0.045, 0.075, 0.085])

    channel = rng.choice(CHANNELS, n_o, p=CHANNEL_P)
    loc_i = rng.integers(0, len(LOCATIONS), n_o)

    orders = pd.DataFrame({
        "order_id": [f"O{i:08d}" for i in range(1, n_o + 1)],
        "customer_id": customers["customer_id"].to_numpy()[cust_idx],
        "order_ts": order_ts,
        "status": status,
        "channel": channel,
        "city": [LOCATIONS[i][0] for i in loc_i],
        "state": [LOCATIONS[i][1] for i in loc_i],
        "region": [LOCATIONS[i][2] for i in loc_i],
        "currency": "USD",
    })
    orders["order_date"] = orders["order_ts"].dt.strftime("%Y-%m-%d")
    orders["order_ts"] = orders["order_ts"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # ----------------------------------------------------------------- order_items
    items_per_order = rng.choice([1, 2, 3, 4, 5, 6], n_o, p=[0.34, 0.30, 0.19, 0.10, 0.05, 0.02])
    total_items = int(items_per_order.sum())
    order_pos = np.repeat(np.arange(n_o), items_per_order)

    # product popularity follows a Zipf curve -> a few strong sellers
    prod_rank = 1.0 / np.power(np.arange(1, n_p + 1), 0.9)
    prod_idx = rng.choice(n_p, total_items, p=prod_rank / prod_rank.sum())

    qty = rng.choice([1, 2, 3, 4], total_items, p=[0.72, 0.20, 0.06, 0.02])
    base_price = products["unit_price"].to_numpy()[prod_idx]
    base_cost = products["unit_cost"].to_numpy()[prod_idx]
    disc_pct = rng.choice([0.0, 0.05, 0.10, 0.15, 0.20, 0.25], total_items,
                          p=[0.52, 0.14, 0.14, 0.10, 0.07, 0.03])

    line_subtotal = np.round(base_price * qty, 2)
    line_total = np.round(line_subtotal * (1 - disc_pct), 2)

    order_items = pd.DataFrame({
        "order_item_id": [f"OI{i:09d}" for i in range(1, total_items + 1)],
        "order_id": orders["order_id"].to_numpy()[order_pos],
        "product_id": products["product_id"].to_numpy()[prod_idx],
        "quantity": qty,
        "unit_price": base_price,
        "unit_cost": base_cost,
        "discount_pct": disc_pct,
        "line_subtotal": line_subtotal,
        "line_total": line_total,
    })

    # roll item totals up to the order header (single source of truth)
    agg = order_items.groupby("order_id").agg(
        item_count=("quantity", "sum"),
        distinct_products=("product_id", "nunique"),
        subtotal=("line_subtotal", "sum"),
        item_total=("line_total", "sum"),
    )
    orders = orders.merge(agg, left_on="order_id", right_index=True, how="left")

    orders["discount_amount"] = np.round(orders["subtotal"] - orders["item_total"], 2)
    shipping = rng.choice([0.0, 4.99, 7.99, 9.99, 14.99], n_o, p=[0.42, 0.24, 0.16, 0.12, 0.06])
    orders["shipping_cost"] = np.where(orders["status"] == "cancelled", 0.0, shipping)
    orders["tax_amount"] = np.round(orders["item_total"] * 0.0825, 2)
    orders["order_total"] = np.where(
        orders["status"] == "cancelled",
        0.0,
        np.round(orders["item_total"] + orders["shipping_cost"] + orders["tax_amount"], 2),
    )
    orders["updated_at"] = orders["order_ts"]

    # -------------------------------------------------------------------- payments
    pay_rows = []
    pid = 0
    for i, (oid, amt, st) in enumerate(zip(orders["order_id"], orders["order_total"], orders["status"])):
        n_attempts = 1 if rng.random() > 0.12 else 2
        method = PAYMENT_METHODS[int(rng.choice(len(PAYMENT_METHODS), p=PAYMENT_P))]
        base_dt = pd.Timestamp(orders["order_ts"].iloc[i])
        for attempt in range(1, n_attempts + 1):
            pid += 1
            if st == "cancelled":
                p_status, reason = "failed", "order_cancelled"
            elif st == "pending":
                p_status, reason = "pending", None
            elif attempt < n_attempts:
                p_status, reason = "failed", str(rng.choice(["insufficient_funds", "gateway_timeout", "card_declined"]))
            elif st == "returned":
                p_status, reason = "refunded", None
            else:
                p_status = "success" if rng.random() > 0.045 else "failed"
                reason = None if p_status == "success" else str(rng.choice(["insufficient_funds", "card_declined"]))
            pay_rows.append({
                "payment_id": f"PAY{pid:09d}",
                "order_id": oid,
                "payment_ts": (base_dt + pd.Timedelta(minutes=2 * attempt + int(rng.integers(0, 3)))).strftime("%Y-%m-%d %H:%M:%S"),
                "payment_method": method,
                "amount": float(amt),
                "status": p_status,
                "gateway": str(rng.choice(["stripe", "adyen", "paypal", "razorpay"])),
                "failure_reason": reason,
                "attempt_number": attempt,
            })
    payments = pd.DataFrame(pay_rows)

    # ------------------------------------------------------------------- shipments
    ship_rows = []
    sid = 0
    for i, (oid, st) in enumerate(zip(orders["order_id"], orders["status"])):
        if st in ("cancelled", "pending"):
            continue
        sid += 1
        ship_dt = pd.Timestamp(orders["order_ts"].iloc[i]) + pd.Timedelta(hours=int(rng.integers(4, 96)))
        transit = int(rng.integers(1, 10))
        promised = (ship_dt + pd.Timedelta(days=transit)).normalize()
        delivered = ship_dt + pd.Timedelta(days=transit + int(rng.integers(0, 4)))
        if delivered > end:
            s_status, delivered_ts = "in_transit", None
        elif rng.random() < 0.04:
            s_status, delivered_ts = "delayed", delivered.strftime("%Y-%m-%d %H:%M:%S")
        else:
            s_status, delivered_ts = "delivered", delivered.strftime("%Y-%m-%d %H:%M:%S")
        ship_rows.append({
            "shipment_id": f"SH{sid:08d}",
            "order_id": oid,
            "carrier": str(rng.choice(CARRIERS)),
            "shipped_ts": ship_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "promised_date": promised.strftime("%Y-%m-%d"),
            "delivered_ts": delivered_ts,
            "status": s_status,
            "shipping_cost": float(orders["shipping_cost"].iloc[i]),
            "tracking_number": f"1Z{int(rng.integers(10**9, 10**10))}",
        })
    shipments = pd.DataFrame(ship_rows)

    # --------------------------------------------------------------------- returns
    ret_rows = []
    rid = 0
    returned_orders = set(orders.loc[orders["status"] == "returned", "order_id"])
    items_by_order = order_items.groupby("order_id")
    for oid in returned_orders:
        grp = items_by_order.get_group(oid)
        # 1..all items returned
        n_ret = max(1, int(rng.integers(1, len(grp) + 1)))
        picked = grp.sample(n=n_ret, random_state=int(rng.integers(0, 10**6)))
        order_ts = pd.Timestamp(orders.loc[orders["order_id"] == oid, "order_ts"].iloc[0])
        for _, it in picked.iterrows():
            rid += 1
            ret_ts = order_ts + pd.Timedelta(days=int(rng.integers(6, 40)))
            ret_rows.append({
                "return_id": f"RT{rid:08d}",
                "order_id": oid,
                "order_item_id": it["order_item_id"],
                "product_id": it["product_id"],
                "return_ts": ret_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "reason": str(rng.choice(RETURN_REASONS)),
                "quantity": int(it["quantity"]),
                "refund_amount": round(float(it["line_total"]), 2),
                "status": str(rng.choice(["completed", "approved", "pending", "rejected"],
                                        p=[0.72, 0.16, 0.08, 0.04])),
                "restocked": bool(rng.random() > 0.35),
            })
    returns = pd.DataFrame(ret_rows)

    # ----------------------------------------------------------------------- write
    tables = {
        "customers": customers, "products": products, "orders": orders,
        "order_items": order_items, "payments": payments,
        "shipments": shipments, "returns": returns,
    }
    manifest = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "seed": settings.random_seed,
        "window": {"start": str(start), "end": str(end)},
        "tables": {},
    }
    for name, df in tables.items():
        d = out / name
        d.mkdir(parents=True, exist_ok=True)
        df.to_csv(d / f"{name}.csv", index=False)
        manifest["tables"][name] = {"rows": int(len(df)), "columns": list(df.columns)}

    # JSON variants exercise the multi-format ingestion path
    for name in ("customers", "payments"):
        d = out / name
        tables[name].to_json(d / f"{name}.json", orient="records", lines=True, date_format="iso")

    (out / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


if __name__ == "__main__":
    m = generate()
    total = sum(v["rows"] for v in m["tables"].values())
    print(f"Generated {len(m['tables'])} tables, {total:,} total rows")
    for k, v in m["tables"].items():
        print(f"  {k:<12} {v['rows']:>9,} rows")
