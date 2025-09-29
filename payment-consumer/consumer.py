import json
import pika
import uuid
from events.base_event import BaseEvent
from events.payment.reserve_payment_event import ReservePaymentEvent
from events.payment.reserve_payment_successfull import ReservePaymentSucessfull
from events.stock.reserve_stock_successful_event import ReserveStockSucessfull
import redis
import os
import ast
import logging
import sys
from msgspec import msgpack, Struct

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

# define channels
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='rabbitmq', port=5672, heartbeat=600, blocked_connection_timeout=300))
channel = connection.channel()
channel.queue_declare(queue="payment", durable=True)
channel.queue_declare(queue="order", durable=True)

db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


DB_ERROR_STR = "DB error"

class UserValue(Struct):
    credit: int

def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        raise Exception("Db error")
    # deserialize data if it exists else return null
    entry: UserValue | None = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        raise Exception(f"User: {user_id} not found!")
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

def reserve_money(reserve_event: ReservePaymentEvent):
    user_entry: UserValue = get_user_from_db(reserve_event.user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(reserve_event.amount)
    if user_entry.credit < 0:
        raise Exception("Not enough credit")
    try:
        db.set(reserve_event.user_id, msgpack.encode(user_entry))
        publish_order_event(
            ReservePaymentSucessfull(
                order_id=reserve_event.order_id
            )
        )
    except redis.exceptions.RedisError:
        ## rollback, or something else?
        pass


def callback(ch, method, properties, body: bytes):
    decoded_body = body.decode()
    params = ast.literal_eval(decoded_body)
    
    try:
        
        if params.get("name", "") == ReservePaymentEvent.name:
            event = ReservePaymentEvent(**params)
            reserve_money(event)
            ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.warning(str(e))

channel.basic_consume(queue="payment", on_message_callback=callback)
channel.start_consuming()

