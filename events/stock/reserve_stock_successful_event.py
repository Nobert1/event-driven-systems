
from typing import ClassVar
from events.base_event import BaseEvent

class ReserveStockSucessfull(BaseEvent):
    name: ClassVar[str] = 'reserve stock successfull'
    order_id: str