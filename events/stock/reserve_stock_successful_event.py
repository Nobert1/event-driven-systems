
from events.base_event import BaseEvent

class ReserveStockSucessfull(BaseEvent):
    name = 'reserve stock successfull'
    order_id: str