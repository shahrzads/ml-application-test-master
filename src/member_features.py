from pydantic import BaseModel
from typing import Optional


class MemberFeatures(BaseModel):
    AVG_POINTS_BOUGHT: Optional[float] = 0.0
    AVG_REVENUE_USD: Optional[float] = 0.0
    LAST_3_TRANSACTIONS_AVG_POINTS_BOUGHT: Optional[float] = 0.0
    LAST_3_TRANSACTIONS_AVG_REVENUE_USD: Optional[float] = 0.0
    PCT_BUY_TRANSACTIONS: Optional[float] = 0.0
    PCT_GIFT_TRANSACTIONS: Optional[float] = 0.0
    PCT_REDEEM_TRANSACTIONS: Optional[float] = 0.0
    DAYS_SINCE_LAST_TRANSACTION: Optional[int] = 0
