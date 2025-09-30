import pika
import redis
import os
from events.base_event import BaseEvent
from events.stock.reserve_stock_event import ReserveStockEvent
from events.stock.reserve_stock_successful_event import ReserveStockSucessfull
import logging
import ast
from msgspec import msgpack, Struct


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Stock consumer started")

DB_ERROR_STR = "DB error"


db: redis.Redis = redis.Redis(host=os.environ['REDIS_HOST'],
                              port=int(os.environ['REDIS_PORT']),
                              password=os.environ['REDIS_PASSWORD'],
                              db=int(os.environ['REDIS_DB']))


def close_db_connection():
    db.close()



# define channels
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='rabbitmq', port=5672, heartbeat=600, blocked_connection_timeout=300))
channel = connection.channel()
channel.queue_declare(queue="stock", durable=True)
channel.queue_declare(queue="order", durable=True)

def publish_order_event(event: BaseEvent):
    channel.basic_publish(
        exchange="",  
        routing_key="order",  
        body=event.to_message_queue_body(),  
        properties=pika.BasicProperties(
            delivery_mode=2  
        )
    )

class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue | None:
    # get serialized data
    try:
        entry: bytes = db.get(item_id)
    except redis.exceptions.RedisError:
        raise Exception("Db error")
    # deserialize data if it exists else return null
    entry: StockValue | None = msgpack.decode(entry, type=StockValue) if entry else None
    if entry is None:
        # if item does not exist in the database; abort
        raise Exception(f"stock item: {item_id} not found!")
    return entry



def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock, serialize and update database
    item_entry.stock += int(amount)
    try:
        db.set(item_id, msgpack.encode(item_entry))
    except redis.exceptions.RedisError:
        raise Exception("Something went wrong")

def remove_stock(event: ReserveStockEvent):
    try:
        updated_stock_items = []
        logger.info(event.model_dump())
        for event_stock_item in event.stock_items:
            db_stock_item = get_item_from_db(event_stock_item.item_id)
            logger.info(db_stock_item)
            if db_stock_item.stock >= event_stock_item.quantity:
                updated_stock_items.append((
                    event_stock_item.item_id, msgpack.encode(StockValue(stock=db_stock_item.stock - event_stock_item.quantity, price=db_stock_item.price))
                ))
            else:
                raise Exception("Not enough stock, now what?")
        logger.info(updated_stock_items)
        for item in updated_stock_items:
            db.set(
                item[0], item[1]
            )
        publish_order_event(
            ReserveStockSucessfull(
                    order_id=event.order_id
            )
        )
    except Exception as e:
        logger.error(str(e))

def callback(ch, method, properties, body: bytes):
    decoded_body = body.decode()
    params = ast.literal_eval(decoded_body)
    try:
        
        if params.get("name", "") == ReserveStockEvent.name:
            event = ReserveStockEvent(**params)
            remove_stock(event)
            ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.warning(str(e))


channel.basic_consume(queue="stock", on_message_callback=callback)
channel.start_consuming()
