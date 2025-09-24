from typing import ClassVar
from events.base_event import BaseEvent
from pydantic import BaseModel

class StockItem(BaseModel):
    itemId: str
    quantity: int

class ReserveStock(BaseEvent):
    name: ClassVar[str] = 'reserve stock'
    order_id: str
    stock_items: list[StockItem]