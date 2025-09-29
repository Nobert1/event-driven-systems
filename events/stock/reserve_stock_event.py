from typing import ClassVar
from events.base_event import BaseEvent
from pydantic import BaseModel

class StockItem(BaseModel):
    item_id: str
    quantity: int

class ReserveStockEvent(BaseEvent):
    name = 'reserve stock'
    order_id: str
    stock_items: list[StockItem]