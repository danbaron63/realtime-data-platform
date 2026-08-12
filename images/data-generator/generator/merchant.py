from dataclasses import dataclass
from generator.base import BaseDatabase, BaseEntity
from datetime import datetime
import random

merchant_categories = [
    "GROCERY",
    "RESTAURANT",
    "CAFE",
    "FUEL",
    "RETAIL",
    "TRAVEL",
    "HOTEL",
    "ENTERTAINMENT",
    "ONLINE_MARKETPLACE",
    "SUBSCRIPTION",
    "ATM",
]


@dataclass
class Merchant(BaseEntity):
    id: str
    merchant_name: str
    merchant_category: str
    country: str
    created_at: datetime


class MerchantDatabase(BaseDatabase):
    def __init__(self):
        super().__init__(Merchant)

    def merchant_onboarded(self) -> Merchant:
        merchant = Merchant(
            id=self._get_next_id(),
            merchant_name=self._faker.company(),
            merchant_category=random.choice(merchant_categories),
            country=self._faker.country(),
            created_at=datetime.now(),
        )
        self._insert(merchant)
        return merchant
