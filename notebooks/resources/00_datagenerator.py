# Databricks notebook source
# DBTITLE 1,Python Generator
import json, ssl, random, time, uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer

# --- Read config from spark.conf (set by Scala in Section 1) ---
kafka_servers = spark.conf.get("demo.kafka.brokers.tls")
input_topic   = spark.conf.get("demo.topic.input")

# --- SSL context for AWS MSK ---
ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# --- Create Kafka producer ---
producer = KafkaProducer(
    bootstrap_servers=kafka_servers,
    security_protocol="SSL",
    ssl_context=ssl_context,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    request_timeout_ms=10000,
    api_version_auto_timeout_ms=10000
)

# --- Constants ---
CARD_IDS = [f"card_{str(i).zfill(4)}" for i in range(1, 6)]
MERCHANT_IDS = [f"merch_{str(i).zfill(4)}" for i in range(1, 11)]
DEVICE_FINGERPRINTS = [f"dfp_{uuid.uuid4().hex[:8]}" for _ in range(50)]
US_CITIES = [
    {"name": "New York",    "lat": 40.7128, "lon": -74.0060},
    {"name": "Los Angeles", "lat": 34.0522, "lon": -118.2437},
    {"name": "Chicago",     "lat": 41.8781, "lon": -87.6298},
    {"name": "Houston",     "lat": 29.7604, "lon": -95.3698},
    {"name": "Seattle",     "lat": 47.6062, "lon": -122.3321},
    {"name": "Miami",       "lat": 25.7617, "lon": -80.1918}
]

# --- Helper: send one transaction to Kafka ---
def send_transaction(transaction):
    try:
        future = producer.send(input_topic, value=transaction)
        future.get(timeout=10)
        print(f"  Sent: {transaction['transaction_id']} | Card: {transaction['card_id']} | Amount: ${transaction['amount_usd']:.2f}")
    except Exception as e:
        print(f"  Error: {type(e).__name__}: {e}")

# --- Normal transaction generator ---
def generate_normal_transaction():
    city = random.choice(US_CITIES)
    return {
        "transaction_id": f"txn_{uuid.uuid4().hex[:8]}",
        "card_id": random.choice(CARD_IDS),
        "merchant_id": random.choice(MERCHANT_IDS),
        "merchant_category": random.choice(["GROCERY", "GAS", "DINING"]),
        "amount_usd": round(random.uniform(5.0, 150.0), 2),
        "currency": "USD",
        "channel": random.choice(["POS", "ONLINE"]),
        "ip_country": "US",
        "device_fingerprint": random.choice(DEVICE_FINGERPRINTS),
        "latitude": city["lat"] + random.uniform(-0.1, 0.1),
        "longitude": city["lon"] + random.uniform(-0.1, 0.1),
        "event_time": datetime.utcnow().isoformat() + "Z"
    }

# --- Fraud pattern: velocity burst (rapid small txns + big purchase) ---
def generate_velocity_burst(card_id):
    city = random.choice(US_CITIES)
    txns = [{
        "transaction_id": f"txn_{uuid.uuid4().hex[:8]}",
        "card_id": card_id,
        "merchant_id": random.choice(MERCHANT_IDS),
        "merchant_category": "ELECTRONICS",
        "amount_usd": round(random.uniform(0.99, 5.0), 2),
        "currency": "USD",
        "channel": "ONLINE",
        "ip_country": random.choice(["RO", "NG", "CN"]),
        "device_fingerprint": f"dfp_fraud_{uuid.uuid4().hex[:8]}",
        "latitude": city["lat"],
        "longitude": city["lon"],
        "event_time": (datetime.utcnow() + timedelta(seconds=i*5)).isoformat() + "Z"
    } for i in range(5)]
    # Big purchase after the probing transactions
    txns.append({
        "transaction_id": f"txn_{uuid.uuid4().hex[:8]}",
        "card_id": card_id,
        "merchant_id": "merch_0001",
        "merchant_category": "ELECTRONICS",
        "amount_usd": round(random.uniform(800.0, 2000.0), 2),
        "currency": "USD",
        "channel": "ONLINE",
        "ip_country": random.choice(["RO", "NG"]),
        "device_fingerprint": f"dfp_fraud_{uuid.uuid4().hex[:8]}",
        "latitude": city["lat"],
        "longitude": city["lon"],
        "event_time": (datetime.utcnow() + timedelta(seconds=30)).isoformat() + "Z"
    })
    return txns

# --- Fraud pattern: impossible geo travel ---
def generate_geo_impossible(card_id):
    return [
        {
            "transaction_id": f"txn_{uuid.uuid4().hex[:8]}",
            "card_id": card_id,
            "merchant_id": random.choice(MERCHANT_IDS),
            "merchant_category": "DINING",
            "amount_usd": round(random.uniform(50.0, 150.0), 2),
            "currency": "USD",
            "channel": "POS",
            "ip_country": "US",
            "device_fingerprint": random.choice(DEVICE_FINGERPRINTS),
            "latitude": 40.7128, "longitude": -74.0060,  # New York
            "event_time": datetime.utcnow().isoformat() + "Z"
        },
        {
            "transaction_id": f"txn_{uuid.uuid4().hex[:8]}",
            "card_id": card_id,
            "merchant_id": random.choice(MERCHANT_IDS),
            "merchant_category": "ELECTRONICS",
            "amount_usd": round(random.uniform(300.0, 800.0), 2),
            "currency": "GBP",
            "channel": "ONLINE",
            "ip_country": "GB",
            "device_fingerprint": f"dfp_fraud_{uuid.uuid4().hex[:8]}",
            "latitude": 51.5074, "longitude": -0.1278,  # London
            "event_time": (datetime.utcnow() + timedelta(seconds=60)).isoformat() + "Z"
        }
    ]

# --- Fraud pattern: amount spike ---
def generate_amount_spike(card_id):
    return [{
        "transaction_id": f"txn_{uuid.uuid4().hex[:8]}",
        "card_id": card_id,
        "merchant_id": "merch_0005",
        "merchant_category": "JEWELRY",
        "amount_usd": round(random.uniform(2000.0, 5000.0), 2),
        "currency": "USD",
        "channel": "ONLINE",
        "ip_country": random.choice(["RO", "NG", "RU"]),
        "device_fingerprint": f"dfp_fraud_{uuid.uuid4().hex[:8]}",
        "latitude": random.choice(US_CITIES)["lat"],
        "longitude": random.choice(US_CITIES)["lon"],
        "event_time": datetime.utcnow().isoformat() + "Z"
    }]

# --- Fraud pattern: small probe / card testing ---
def generate_small_probe(card_id):
    """$0.50-$3 card test from Nigeria, online gift cards."""
    return [{
        "transaction_id": f"txn_{uuid.uuid4().hex[:8]}",
        "card_id": card_id,
        "merchant_id": "merch_0006",
        "merchant_category": "GIFT_CARDS",
        "amount_usd": round(random.uniform(0.50, 3.0), 2),
        "currency": "USD",
        "channel": "ONLINE",
        "ip_country": "NG",
        "device_fingerprint": f"dfp_fraud_{uuid.uuid4().hex[:8]}",
        "latitude": random.choice(US_CITIES)["lat"],
        "longitude": random.choice(US_CITIES)["lon"],
        "event_time": datetime.utcnow().isoformat() + "Z"
    }]

# --- Convenience: inject a named fraud pattern ---
def inject_fraud_pattern(pattern_type):
    card_id = random.choice(CARD_IDS)
    print(f"\nINJECTING FRAUD PATTERN: {pattern_type} on {card_id}")
    patterns = {
        "velocity":    generate_velocity_burst,
        "geo":         generate_geo_impossible,
        "amount":      generate_amount_spike,
        "small_probe": generate_small_probe,
    }
    if pattern_type not in patterns:
        print(f"Unknown pattern: {pattern_type}. Choose from: {list(patterns.keys())}")
        return
    for txn in patterns[pattern_type](card_id):
        send_transaction(txn)
        time.sleep(1)
    print(f"Pattern '{pattern_type}' injected.\n")

# --- Baseline traffic generator ---
def run_baseline_generator(duration_seconds=60, tps=5):
    print(f"Generating baseline traffic for {duration_seconds}s at {tps} TPS...")
    start_time = time.time()
    count = 0
    while time.time() - start_time < duration_seconds:
        send_transaction(generate_normal_transaction())
        count += 1
        time.sleep(1.0 / tps)
    print(f"Baseline generation complete. Sent {count} transactions.")

print("Data generator functions ready.")