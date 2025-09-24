
from events.base_event import BaseEvent

class ReserveStockSucessfull(BaseEvent):
    name = 'reserve payment successfull'
    order_id: str