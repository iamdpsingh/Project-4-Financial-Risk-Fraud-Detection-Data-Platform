from datetime import datetime

from pydantic import BaseModel, Field


class TransactionEvent(BaseModel):
    transaction_id: str = Field(..., description="Unique identifier for the transaction")
    customer_id: str = Field(..., description="Customer ID associated with the transaction")
    account_id: str = Field(..., description="Account ID associated with the transaction")
    merchant_id: str = Field(..., description="Merchant ID where the transaction occurred")
    device_id: str = Field(..., description="Device ID used for the transaction")
    timestamp: datetime = Field(..., description="Timestamp of the transaction")
    amount: float = Field(..., description="Amount of the transaction in local currency", ge=0.01)
    currency: str = Field(..., min_length=3, max_length=3)
    amount_usd: float = Field(..., description="Amount converted to USD", ge=0.01)
    type: str = Field(..., description="Type of transaction (e.g., online, pos, atm, wire)")
    country: str = Field(..., description="Country code (ISO 3166-1 alpha-2) of the transaction location", min_length=2, max_length=2)
    payment_method: str = Field(..., description="Payment method used (e.g., credit_card, debit_card)")
    is_fraud: bool | None = Field(default=None, description="Ground truth flag for fraud (optional for simulation)")
