import json
import uuid
import random
import time
from datetime import datetime, timezone, timedelta

from kafka import KafkaProducer

# ─────────────────────────────
# 1. 기본 설정
# ─────────────────────────────

USER_IDS = [f"user_{i}" for i in range(1, 301)] #100 > 300으로 늘림_재구매 빈도 줄이기 위헤서
PRODUCT_IDS = [f"prod_{i}" for i in range(1, 51)]
EVENT_TYPES = ["view", "search", "add_to_cart", "purchase"]
EVENT_WEIGHTS = [0.5, 0.25, 0.15, 0.1]

SEARCH_KEYWORDS = [
    "laptop",
    "keyboard",
    "monitor",
    "mouse",
    "tablet",
    "iphone case",
    "gaming chair",
    "usb hub",
]

COUNTRIES = ["KR", "US", "JP", "SG"]

def get_base_country(user_id):
    user_num = int(user_id.split("_")[1])
    return COUNTRIES[(user_num - 1) % len(COUNTRIES)]

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "raw-events"

# ─────────────────────────────
# 2. 세션 관리
# ─────────────────────────────

user_sessions = {}


def get_session(user_id, timeout_minutes=30):
    now = datetime.now(timezone.utc)

    if user_id in user_sessions:
        session_id, last_active = user_sessions[user_id]
        if now - last_active < timedelta(minutes=timeout_minutes):
            user_sessions[user_id] = (session_id, now)
            return session_id

    new_session_id = f"sess_{uuid.uuid4().hex[:8]}"
    user_sessions[user_id] = (new_session_id, now)
    return new_session_id

# ─────────────────────────────
# 3. Kafka Producer
# ─────────────────────────────

def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )
    return producer

# ─────────────────────────────
# 4. 이벤트 생성
# ─────────────────────────────

def generate_event():
    user_id = random.choice(USER_IDS)
    session_id = get_session(user_id)
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]

    country = get_base_country(user_id)

    if event_type == "purchase" and random.random() < 0.04:
        country = random.choice([c for c in COUNTRIES if c != country])

    event = {
        "event_id": uuid.uuid4().hex,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "user_id": user_id,
        "session_id": session_id,
        "product_id": None,
        "search_query": None,
        "amount": None,
        "country": country,
    }

    if event_type in ["view", "add_to_cart", "purchase"]:
        event["product_id"] = random.choice(PRODUCT_IDS)

    if event_type == "search":
        event["search_query"] = random.choice(SEARCH_KEYWORDS)

    if event_type == "purchase":
        event["amount"] = round(random.uniform(10, 500), 2)

    return event

# ─────────────────────────────
# 5. 실행
# ─────────────────────────────

if __name__ == "__main__":
    print("🎬 Kafka 이벤트 전송 시작 (Ctrl+C로 종료)")

    producer = create_producer()

    try:
        while True:
            event = generate_event()
            future = producer.send(
                topic=KAFKA_TOPIC,
                key=event["user_id"],
                value=event,
            )
            metadata = future.get(timeout=10)

            print(
                f"sent -> topic={metadata.topic}, partition={metadata.partition}, offset={metadata.offset} | "
                f"{event['event_type']} | user={event['user_id']} | session={event['session_id']}"
            )

            time.sleep(1)

    except KeyboardInterrupt:
        print("\n🛑 종료")
    finally:
        producer.flush()
        producer.close()