"""
Merchant data generator.

Creates merchants across different business categories with realistic names
and risk levels. The category distribution reflects what you'd see in a
typical payment network — lots of retail and grocery, a smaller number of
high-risk categories like crypto exchanges and gambling sites.

Risk levels are assigned based on the merchant category. Compliance teams
at real financial institutions maintain exactly this kind of merchant risk
register, so we're mirroring that here.

Run directly:
    python data/generators/generate_merchants.py
    python data/generators/generate_merchants.py --count 500
"""

import argparse
import csv
import logging
import random
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from config import (
    CUSTOMER_COUNTRIES,
    MERCHANT_CATEGORIES,
    MERCHANT_RISK_MAP,
    NUM_MERCHANTS,
    OUTPUT_DIR,
    RANDOM_SEED,
    SAMPLE_DIR,
    SAMPLE_SIZE,
)
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

# MCC codes for each category. These are real ISO 18245 codes.
CATEGORY_MCC = {
    "Online Retail": "5999",
    "Grocery & Supermarket": "5411",
    "Restaurant & Food Delivery": "5812",
    "Travel & Hotels": "7011",
    "Electronics": "5734",
    "Fuel & Gas Station": "5541",
    "Healthcare & Pharmacy": "5912",
    "Subscription Services": "7372",
    "Entertainment & Streaming": "7994",
    "Money Transfer": "4829",
    "Cryptocurrency Exchange": "6051",
    "Online Gambling": "7995",
    "Luxury Goods": "5944",
    "Gift Cards": "5969",
    "ATM & Cash Advance": "6011",
}

# Merchant name prefixes per category to make names feel category-specific.
CATEGORY_NAME_PREFIXES = {
    "Online Retail": ["ShopFast", "BuyNow", "QuickCart", "MegaStore", "DirectBuy"],
    "Grocery & Supermarket": ["FreshMart", "GreenBasket", "DailyGrocer", "SaveMore"],
    "Restaurant & Food Delivery": ["FoodRush", "QuickBite", "TastyHub", "MealDash"],
    "Travel & Hotels": ["StayEasy", "GlobeInn", "TripNest", "SkyLodge"],
    "Electronics": ["TechZone", "GadgetHub", "DigiMart", "ByteStore"],
    "Fuel & Gas Station": ["FastFuel", "PetroStop", "QuikGas", "FuelPoint"],
    "Healthcare & Pharmacy": ["MedPlus", "HealthFirst", "CarePharma", "WellDrug"],
    "Subscription Services": ["StreamBox", "SubHub", "MonthlyPlan", "AutoRenew"],
    "Entertainment & Streaming": ["PlayVault", "StreamNow", "ClickWatch", "MediaBox"],
    "Money Transfer": ["QuickSend", "TransferGo", "MoneyBridge", "SwiftPay"],
    "Cryptocurrency Exchange": ["CryptoX", "BitSwap", "CoinVault", "ChainTrade"],
    "Online Gambling": ["BetNow", "LuckySpins", "WagerZone", "JackpotHub"],
    "Luxury Goods": ["PremiumVault", "EliteGoods", "LuxeMart", "Prestige"],
    "Gift Cards": ["GiftFirst", "CardZone", "VoucherHub", "GiftLink"],
    "ATM & Cash Advance": ["QuickCash", "ATMPlus", "CashPoint", "InstantATM"],
}


def pick_category(rng: random.Random) -> str:
    categories = list(MERCHANT_CATEGORIES.keys())
    weights = list(MERCHANT_CATEGORIES.values())
    return rng.choices(categories, weights=weights, k=1)[0]


def pick_country(rng: random.Random) -> str:
    countries = list(CUSTOMER_COUNTRIES.keys())
    weights = list(CUSTOMER_COUNTRIES.values())
    return rng.choices(countries, weights=weights, k=1)[0]


def generate_merchant_name(category: str, rng: random.Random, fake: Faker) -> str:
    """
    Build a merchant name that feels appropriate for the category.
    We combine a category-specific prefix with a random suffix so the
    names are varied but still make sense.
    """
    prefix = rng.choice(CATEGORY_NAME_PREFIXES.get(category, ["Merchant"]))
    suffix = rng.choice(["Ltd", "Inc", "Co", "Group", "Global", "Online", "Direct", ""])
    return f"{prefix} {suffix}".strip()


def generate_merchants(count: int, seed: int) -> list[dict]:
    """
    Generate `count` merchants.

    Risk category is derived from the merchant's business category — we don't
    assign it randomly. This is important because the fraud signals later need
    to correctly identify high-risk merchants, and the risk category has to
    be consistent with what kind of business it is.
    """
    rng = random.Random(seed)
    fake = Faker()
    Faker.seed(seed)

    merchants = []
    base_time = datetime(2015, 1, 1, tzinfo=UTC)

    for _i in range(count):
        category = pick_category(rng)
        risk = MERCHANT_RISK_MAP.get(category, "low")
        country = pick_country(rng)
        created_at = base_time + timedelta(days=rng.randint(0, 8 * 365))

        merchants.append({
            "merchant_id": str(uuid.uuid4()),
            "merchant_name": generate_merchant_name(category, rng, fake),
            "merchant_category": category,
            "mcc_code": CATEGORY_MCC.get(category, "5999"),
            "country": country,
            "city": fake.city()[:100],
            "risk_category": risk,
            "created_at": created_at.isoformat(),
            "updated_at": created_at.isoformat(),
        })

    log.info("Generated %d merchants", count)
    return merchants


def save_to_csv(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    log.info("Saved %d records → %s", len(records), path)


def main(count: int, output_dir: Path) -> list[dict]:
    merchants = generate_merchants(count, RANDOM_SEED)
    save_to_csv(merchants, output_dir / "merchants" / "merchants.csv")
    save_to_csv(merchants[:SAMPLE_SIZE], SAMPLE_DIR / "merchants_sample.csv")
    return merchants


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic merchant data")
    parser.add_argument("--count", type=int, default=NUM_MERCHANTS)
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args()
    main(count=args.count, output_dir=args.output)
