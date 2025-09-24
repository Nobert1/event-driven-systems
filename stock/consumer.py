import pika
import requests
import uuid
import redis
import os
from msgspec import msgpack, Struct

DB_ERROR_STR = "DB error"


db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()

class StockValue(Struct):
    stock: int
    price: int


# define channels
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='rabbitmq', port=5672, heartbeat=600, blocked_connection_timeout=300))
channel = connection.channel()
channel.queue_declare(queue="stock", durable=True)

def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        raise Exception("Not found")
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        raise Exception("Not found")
    return entry


def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        raise Exception("Something went wrong")

def remove_stock(item_id: str, amount: int):
    ## Implement
    pass

def add_stock_item(price: int):
    ## Implement
    pass

def remove_stock_item(id: str):
    ## Implement
    pass


def callback(ch, method, properties, body):
    params = body.decode().split(",")
    if params[0] == "inc":
        for i in range(1, len(params)):
            add_stock(params[i], 1)

    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue="stock", on_message_callback=callback)
channel.start_consuming()