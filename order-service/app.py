import json
import logging
import os
import atexit
import random
from typing import Literal
import uuid
from collections import defaultdict

from events.base_event import BaseEvent
from events.payment.reserve_payment_event import ReservePaymentEvent
from events.stock.reserve_stock_event import ReserveStockEvent, StockItem
import redis
import requests

from flask import Flask, jsonify, abort, Response
import pika
from msgspec import msgpack, Struct
import logging
import sys
import ast

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Capture uncaught exceptions and log them
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        # Allow Ctrl+C to still stop the program cleanly
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = handle_exception


DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


# define channels
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='rabbitmq', port=5672, heartbeat=600, blocked_connection_timeout=300))
channel = connection.channel()
channel.queue_declare(queue="stock", durable=True)
channel.queue_declare(queue="payment", durable=True)
channel.queue_declare(queue="order", durable=True)

def close_db_connection():
    db.close()

atexit.register(close_db_connection)


class OrderValue(Struct):
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int
    payment_status: Literal['pending', 'approved', 'rejected']
    stock_status: Literal['pending', 'approved', 'rejected']

def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(order_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: OrderValue | None = msgpack.decode(entry, type=OrderValue) if entry else None
    if entry is None:
        # if order does not exist in the database; abort
        abort(400, f"Order: {order_id} not found!")
    return entry


@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    value = msgpack.encode(OrderValue(payment_status='pending', stock_status='pending', items=[], user_id=user_id, total_cost=0))
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):

    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

    def generate_entry() -> OrderValue:
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        value = OrderValue(payment_status='pending', stock_status='pending',
                           items=[(f"{item1_id}", 1), (f"{item2_id}", 1)],
                           user_id=f"{user_id}",
                           total_cost=2*item_price)
        return value

    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(generate_entry())
                                  for i in range(n)}
    try:
        db.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost,
            "payment_status": order_entry.payment_status,
            "stock_status": order_entry.stock_status
        }
    )


def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    try:
        db.set(order_id, msgpack.encode(order_entry))
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


def publish_payment_event(event: BaseEvent):
    channel.basic_publish(
        exchange="",  
        routing_key="payment",  
        body=event.to_message_queue_body(),  
        properties=pika.BasicProperties(
            delivery_mode=2  
        )
    )
    

def publish_stock_event(event: BaseEvent):
    channel.basic_publish(
        exchange="",  
        routing_key="stock",  
        body=event.to_message_queue_body(),  
        properties=pika.BasicProperties(
            delivery_mode=2  
        )
    )


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    try:
        logger.info(f"Checking out {order_id}")
        order_entry: OrderValue = get_order_from_db(order_id)
        payment_event = ReservePaymentEvent(
                amount=order_entry.total_cost,
                user_id=order_entry.user_id,
                order_id=order_id
            )
        publish_payment_event(
            payment_event
        )
        stock_event = ReserveStockEvent(
                order_id=order_id,
                stock_items=[
                    StockItem(
                        item_id=item_id, quantity=quanitity
                    ) for item_id, quanitity in order_entry.items
                ]
            )
        publish_stock_event(
            stock_event
        )
        logger.info("checked out order")
    except Exception as e:
        return str(e)
    return Response("CHeckout successfull", 200)

@app.get("/")
def healthcheck():
    return "OK"


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
