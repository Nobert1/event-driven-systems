
from events.base_event import BaseEvent
from typing import ClassVar

class ReservePaymentSucessfull(BaseEvent):
    name: ClassVar[str] = 'reserve payment successfull'
    order_id: str