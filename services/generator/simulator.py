import random
import uuid
from datetime import datetime, timezone

from schemas import POSTransaction

# ── Vinamilk product catalog ───────────────────────────────────────────────────
PRODUCTS = [
    {"id": "SKU_001", "name": "Vinamilk Tuoi Tiet Trung 1L",        "category": "dairy",          "min": 28000,  "max": 35000},
    {"id": "SKU_002", "name": "Vinamilk Tuoi Thanh Trung 500ml",     "category": "dairy",          "min": 18000,  "max": 22000},
    {"id": "SKU_003", "name": "Vinamilk Organic Tuoi 180ml",         "category": "dairy",          "min": 10000,  "max": 14000},
    {"id": "SKU_004", "name": "Vinamilk ADM GOLD 900g",              "category": "dairy",          "min": 320000, "max": 380000},
    {"id": "SKU_005", "name": "Sua Chua Vinamilk Co Duong 100g",     "category": "yogurt",         "min": 7000,   "max": 10000},
    {"id": "SKU_006", "name": "Sua Chua Vinamilk Khong Duong 100g",  "category": "yogurt",         "min": 7000,   "max": 10000},
    {"id": "SKU_007", "name": "Sua Chua Uong Vinamilk 130ml",        "category": "drinking_yogurt","min": 9000,   "max": 13000},
    {"id": "SKU_008", "name": "Sua Chua Uong Probi 130ml",           "category": "drinking_yogurt","min": 10000,  "max": 14000},
    {"id": "SKU_009", "name": "Nuoc Ep Cam Vfresh 1L",               "category": "juice",          "min": 35000,  "max": 45000},
    {"id": "SKU_010", "name": "Nuoc Ep Buoi Vfresh 200ml",           "category": "juice",          "min": 10000,  "max": 15000},
    {"id": "SKU_011", "name": "Sua Dac Ngoi Sao Phuong Nam 380g",   "category": "condensed_milk", "min": 25000,  "max": 32000},
    {"id": "SKU_012", "name": "Sua Dac Vinamilk Ong Tho 380g",      "category": "condensed_milk", "min": 26000,  "max": 33000},
]

# ── Weighted region distribution (reflect actual Vinamilk sales) ───────────────
REGIONS        = ["HN",  "HCM", "DN",  "CT",  "HP",  "BD"]
REGION_WEIGHTS = [0.28,  0.35,  0.12,  0.10,  0.08,  0.07]

STORE_TYPES        = ["supermarket", "convenience", "wet_market", "mini_mart"]
STORE_WEIGHTS      = [0.30,           0.35,           0.25,          0.10]

POS_COUNT = 1000


def _round_price(value: float) -> float:
    """Round price to nearest 500 VND."""
    return round(value / 500) * 500


def generate_transaction() -> POSTransaction:
    product  = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    unit_price   = _round_price(random.uniform(product["min"], product["max"]))
    total_amount = round(quantity * unit_price, 2)

    return POSTransaction(
        transaction_id=str(uuid.uuid4()),
        pos_id=f"POS_{random.randint(1, POS_COUNT):04d}",
        product_id=product["id"],
        product_name=product["name"],
        category=product["category"],
        quantity=quantity,
        unit_price=unit_price,
        total_amount=total_amount,
        region=random.choices(REGIONS, weights=REGION_WEIGHTS, k=1)[0],
        store_type=random.choices(STORE_TYPES, weights=STORE_WEIGHTS, k=1)[0],
        timestamp=datetime.now(timezone.utc),
    )


def generate_batch(size: int) -> list[POSTransaction]:
    return [generate_transaction() for _ in range(size)]
