from pydantic import BaseModel
from typing import Literal
from datetime import datetime


class POSTransaction(BaseModel):
    transaction_id: str
    pos_id: str
    product_id: str
    product_name: str
    category: Literal["dairy", "yogurt", "drinking_yogurt", "juice", "condensed_milk"]
    quantity: int
    unit_price: float
    total_amount: float
    region: Literal["HN", "HCM", "DN", "CT", "HP", "BD"]
    store_type: Literal["supermarket", "convenience", "wet_market", "mini_mart"]
    timestamp: datetime


class SimulateRequest(BaseModel):
    count: int = 100


class StreamStatusResponse(BaseModel):
    streaming: bool
    tps: int = 0
