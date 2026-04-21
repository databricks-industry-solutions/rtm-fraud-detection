# Databricks notebook source
# DBTITLE 1,Shared Reference Data (Merchant + Card Profiles)
from pyspark.sql.types import *
import pyspark.sql.functions as F

# --- Merchant Reference Table ---
# Each merchant has a risk_tier: gift card sellers and foreign electronics
# stores are flagged as higher risk than grocery stores.
merchant_data = spark.createDataFrame([
    ("merch_0001", "TechMart Online",    "ELECTRONICS", "HIGH",      "US"),
    ("merch_0002", "GroceryHub",         "GROCERY",     "LOW",       "US"),
    ("merch_0003", "FuelStop Gas",       "GAS",         "LOW",       "US"),
    ("merch_0004", "DineOut Restaurant", "DINING",      "MEDIUM",    "US"),
    ("merch_0005", "LuxuryJewels",       "JEWELRY",     "HIGH",      "US"),
    ("merch_0006", "QuickGifts Online",  "GIFT_CARDS",  "VERY_HIGH", "RO"),
    ("merch_0007", "GlobalElectronics",  "ELECTRONICS", "HIGH",      "CN"),
    ("merch_0008", "CoffeeShop",         "DINING",      "LOW",       "US"),
    ("merch_0009", "OnlinePharmacy",     "HEALTHCARE",  "MEDIUM",    "US"),
    ("merch_0010", "TravelBooking",      "TRAVEL",      "MEDIUM",    "US"),
], ["merchant_id", "merchant_name", "merchant_category", "risk_tier", "merchant_country"])

merchant_data.createOrReplaceTempView("merchants")
spark.catalog.cacheTable("merchants")

# --- Card Profile Reference Table ---
# Each card has a spending profile: home country, average transaction amount,
# 30-day max, and typical merchant categories.
_card_profile_schema = StructType([
    StructField("card_id", StringType()),
    StructField("home_country", StringType()),
    StructField("avg_txn_amount", DoubleType()),
    StructField("max_txn_30d", DoubleType()),
    StructField("risk_segment", StringType()),
    StructField("typical_categories", ArrayType(StringType())),
])

card_profile_data = spark.createDataFrame([
    ("card_0001", "US", 65.0,  320.0, "LOW",    ["GROCERY", "GAS", "DINING"]),
    ("card_0002", "US", 120.0, 450.0, "LOW",    ["ELECTRONICS", "GROCERY", "DINING"]),
    ("card_0003", "US", 45.0,  200.0, "LOW",    ["GROCERY", "GAS"]),
    ("card_0004", "GB", 85.0,  380.0, "MEDIUM", ["TRAVEL", "DINING", "ELECTRONICS"]),
    ("card_0005", "US", 50.0,  250.0, "LOW",    ["GROCERY", "GAS", "DINING"]),
], _card_profile_schema)

card_profile_data.createOrReplaceTempView("card_profiles")
spark.catalog.cacheTable("card_profiles")

# --- Risk tier numeric mapping (for ML features) ---
RISK_TIER_MAP = {"LOW": 0, "MEDIUM": 20, "HIGH": 50, "VERY_HIGH": 80}

# COMMAND ----------

# DBTITLE 1,Verify reference data
print(f"""
Reference Data Loaded
=====================
  Merchants:     {merchant_data.count()} rows (cached as 'merchants' temp view)
  Card Profiles: {card_profile_data.count()} rows (cached as 'card_profiles' temp view)

Merchant columns:     {merchant_data.columns}
Card Profile columns: {card_profile_data.columns}
""")