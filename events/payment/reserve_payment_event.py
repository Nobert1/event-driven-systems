from typing import ClassVar
from events.base_event import BaseEvent

class ReservePaymentEvent(BaseEvent):
    name: ClassVar[str] = 'Reserve payment'
    amount: float
    user_id: str
    order_id: str