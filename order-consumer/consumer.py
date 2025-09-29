import json
from typing import Literal
import pika
from events.base_event import BaseEvent
from events.payment.reserve_payment_event import ReservePaymentEvent
from events.payment.reserve_payment_successfull import ReservePaymentSucessfull
from events.stock.reserve_stock_successful_event import ReserveStockSucessfull
import redis
import os
import ast
from pydantic import BaseModel
import os
import uuid
from pika.spec import Basic, BasicProperties
import redis
import logging
from msgspec import msgpack, Struct

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Order consumer started")


# define channels
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='rabbitmq', port=5672, heartbeat=600, blocked_connection_timeout=300))
channel = connection.channel()
channel.queue_declare(queue="payment", durable=True)
channel.queue_declare(queue="order", durable=True)
channel.queue_declare(queue="stock", durable=True)

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


DB_ERROR_STR = "DB error"

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

def publish_order_event(event: BaseEvent):
    channel.basic_publish(
        exchange="",  
        routing_key="order",  
        body=event.to_message_queue_body(),  
        properties=pika.BasicProperties(
            delivery_mode=2  
        )
    )

def publish_payment_event(event: BaseEvent):
    channel.basic_publish(
        exchange="",  
        routing_key="order",  
        body=event.to_message_queue_body(),  
        properties=pika.BasicProperties(
            delivery_mode=2  
        )
    )


def callback(ch, method, properties: BasicProperties, body: bytes):
    params = ast.literal_eval(body.decode())
    try:
        if params.get("name", "") == ReserveStockSucessfull.name:
            event = ReserveStockSucessfull(**params)
            order = get_order_from_db(event.order_id)
            order.stock_status = 'approved'
            order_entry = msgpack.encode(order)
            db.set(event.order_id, order_entry)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        if params.get("name", "") == ReservePaymentSucessfull.name:
            event = ReservePaymentSucessfull(**params)
            order = get_order_from_db(event.order_id)
            order.payment_status = 'approved'
            order_entry = msgpack.encode(order)
            db.set(event.order_id, order_entry)
            ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.warning(str(e))
        

channel.basic_consume(queue="order", on_message_callback=callback)
channel.start_consuming()