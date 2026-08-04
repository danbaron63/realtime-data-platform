from dataclasses import dataclass
from generator.base import BaseDatabase, BaseEntity
from datetime import datetime
from typing import Iterable
import random


@dataclass
class Withdrawal(BaseEntity):
    id: int
    timestamp: datetime
    customer_id: int
    account_id: int
    card_id: int
    atm_id: int
    amount: float
    currency: str

    @classmethod
    def get_constraints(cls) -> Iterable[str]:
        return [
            "FOREIGN KEY (customer_id) REFERENCES customer(id)",
            "FOREIGN KEY (account_id) REFERENCES account(id)",
            "FOREIGN KEY (card_id) REFERENCES card(id)",
        ]


class AtmDatabase(BaseDatabase):
    def __init__(self, account_db, card_db):
        super().__init__(Withdrawal)
        self._account_db = account_db
        self._card_db = card_db

    def atm_withdrawal(self) -> Withdrawal | None:
        card = self._card_db.get_random()
        account = self._account_db.get_item(card.account_id)

        withdrawal = Withdrawal(
            id=self._get_next_id(),
            timestamp=datetime.now(),
            customer_id=card.customer_id,
            account_id=card.account_id,
            card_id=card.id,
            amount=random.randint(100, 30000) / 100,
            currency=account.currency,
            atm_id=random.randint(0, 1000),
        )

        self._insert(withdrawal)
        return withdrawal
