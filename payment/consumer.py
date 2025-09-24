import json
import pika
import uuid
from events.base_event import BaseEvent
from events.payment.reserve_payment_event import ReservePaymentEvent
from events.stock.reserve_stock_successful_event import ReserveStockSucessfull
import redis
import os
import ast
from pydantic import BaseModel
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

class UserValue(BaseModel):
    credit: int

def get_user_from_db(user_id: str) -> UserValue:
    try:
        # get serialized data
        entry: bytes = db.get(user_id)
    except redis.exceptions.RedisError:
        raise Exception("Redis exception")
    # deserialize data if it exists else return null
    entry: UserValue | None = UserValue(**entry)
    if entry is None:
        # if user does not exist in the database; abort
        raise Exception("User not found")
    return entry

def publish_order_event(event: BaseEvent):
    channel.basic_publish(
        exchange="",  
        routing_key="order",  
        body=json.dumps(event),  
        properties=pika.BasicProperties(
            delivery_mode=2  
        )
    )

def add_money(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit, serialize and update database
    user_entry.credit += int(amount)
    try:
        db.set(user_id, user_entry.model_dump())
    except redis.exceptions.RedisError:
        pass ## What happens now?

def reserve_money(reserve_event: ReservePaymentEvent):
    user_entry: UserValue = get_user_from_db(reserve_event.user_id)
    # update credit, serialize and update database
    user_entry.credit -= int(reserve_event.amount)
    if user_entry.credit < 0:
        raise Exception("Not enough credit")
    try:
        db.set(reserve_event.user_id, user_entry.model_dump())
        publish_order_event(
        ReserveStockSucessfull(
            order_id=reserve_event.order_id
        )
    )
    except redis.exceptions.RedisError:
        ## rollback, or something else?
        pass

def create_user():
    key = str(uuid.uuid4())
    value = UserValue(credit=0)
    try:
        db.set(key, value)
    except redis.exceptions.RedisError:
        ## What happens when this goes wrong?
        pass




def callback(ch, method, properties, body):
    params = ast.literal_eval(body.decode())
    if params.name == ReservePaymentEvent.name:
        event = ReservePaymentEvent(**params)
        reserve_money(event)
        ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue="payment", on_message_callback=callback)
channel.start_consuming()