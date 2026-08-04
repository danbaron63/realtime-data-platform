from dataclasses import dataclass
from generator.base import BaseDatabase, BaseEntity
from datetime import datetime
from typing import Iterable
import random


channels = [
    "CARD_PRESENT",
    "CARD_NOT_PRESENT",
]


@dataclass
class Payment(BaseEntity):
    id: int
    timestamp: datetime
    customer_id: int
    account_id: int
    card_id: int
    merchant_id: int
    amount: float
    currency: str
    channel: str

    @classmethod
    def get_constraints(cls) -> Iterable[str]:
        return [
            "FOREIGN KEY (customer_id) REFERENCES customer(id)",
            "FOREIGN KEY (account_id) REFERENCES account(id)",
            "FOREIGN KEY (card_id) REFERENCES card(id)",
            "FOREIGN KEY (merchant_id) REFERENCES merchant(id)",
        ]


class PaymentDatabase(BaseDatabase):
    def __init__(self, account_db, card_db, merchant_db):
        super().__init__(Payment)
        self._account_db = account_db
        self._card_db = card_db
        self._merchant_db = merchant_db

    def payment_authorised(self) -> Payment | None:
        card = self._card_db.get_random()
        merchant = self._merchant_db.get_random()
        account = self._account_db.get_item(card.account_id)

        payment = Payment(
            id=self._get_next_id(),
            timestamp=datetime.now(),
            customer_id=card.customer_id,
            account_id=card.account_id,
            card_id=card.id,
            merchant_id=merchant.id,
            amount=random.randint(100, 1000000) / 100,
            currency=account.currency,
            channel=random.choice(channels),
        )

        self._insert(payment)
        return payment
