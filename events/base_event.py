
from abc import ABC
import ast
from typing import ClassVar
from pydantic import BaseModel

class BaseEvent(ABC, BaseModel):
    name: ClassVar[str]

    def to_message_queue_body(self):
        body = self.model_dump()
        body['name'] = self.name
        return str(body)
    
    @classmethod
    def from_db(cls, entry):
        return cls(**ast.literal_eval(entry.decode("utf-8")))
    
    def to_db_entry(self):
        return str(self.model_dump())