import requests
import json
import time
from datetime import datetime
from google.cloud import pubsub_v1
from dotenv import load_dotenv
import os

load_dotenv()

PROJECT_ID = "retail-stream-pipeline"
TOPIC_ID = "retail-orders"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def fetch_products():
    response = requests.get("https://fakestoreapi.com/products")
    return response.json()

def fetch_users():
    response = requests.get("https://fakestoreapi.com/users")
    return response.json()

def fetch_carts():
    response = requests.get("https://fakestoreapi.com/carts")
    return response.json()

def fetch_exchange_rates():
    response = requests.get("https://open.er-api.com/v6/latest/USD")
    data = response.json()
    return data.get("rates", {})

def build_order_event(cart, users, products, rates):
    user = next((u for u in users if u["id"] == cart["userId"]), {})
    items = []
    for item in cart["products"]:
        product = next((p for p in products if p["id"] == item["productId"]), {})
        items.append({
            "product_id": item["productId"],
            "product_title": product.get("title", "unknown"),
            "category": product.get("category", "unknown"),
            "quantity": item["quantity"],
            "unit_price_usd": product.get("price", 0),
            "total_price_usd": round(item["quantity"] * product.get("price", 0), 2)
        })

    total_usd = sum(i["total_price_usd"] for i in items)
    inr_rate = rates.get("INR", 83.0)

    return {
        "order_id": f"ORD-{cart['id']}-{int(time.time())}",
        "user_id": cart["userId"],
        "user_name": f"{user.get('name', {}).get('firstname', '')} {user.get('name', {}).get('lastname', '')}".strip(),
        "user_email": user.get("email", ""),
        "user_city": user.get("address", {}).get("city", ""),
        "order_date": cart.get("date", datetime.utcnow().isoformat()),
        "items": items,
        "total_amount_usd": round(total_usd, 2),
        "total_amount_inr": round(total_usd * inr_rate, 2),
        "currency_rate_usd_inr": inr_rate,
        "ingested_at": datetime.utcnow().isoformat()
    }

def publish_event(event):
    data = json.dumps(event).encode("utf-8")
    future = publisher.publish(topic_path, data)
    print(f"Published order {event['order_id']} | USD: {event['total_amount_usd']} | INR: {event['total_amount_inr']}")
    return future.result()

def run():
    print("Starting producer...")
    print("Fetching reference data...")
    products = fetch_products()
    users = fetch_users()
    rates = fetch_exchange_rates()
    print(f"Fetched {len(products)} products, {len(users)} users, exchange rates loaded")

    round_num = 0
    while True:
        round_num += 1
        print(f"\n--- Round {round_num} | {datetime.utcnow().isoformat()} ---")
        carts = fetch_carts()
        for cart in carts:
            event = build_order_event(cart, users, products, rates)
            publish_event(event)
        print(f"Sleeping 60 seconds...")
        time.sleep(60)

if __name__ == "__main__":
    run()