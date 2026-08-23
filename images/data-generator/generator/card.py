from dataclasses import dataclass
from datetime import UTC, datetime

from generator.base import BaseDatabase, BaseEntity

max_cards_per_account = 2


@dataclass
class Card(BaseEntity):
    id: str
    account_id: str
    customer_id: str
    issued_at: datetime


class CardDatabase(BaseDatabase):
    def __init__(self, customer_db, account_db):
        super().__init__(Card)
        self._customer_db = customer_db
        self._account_db = account_db

    def card_issued(self) -> Card:
        # get customer
        account = self._account_db.get_random()

        card = Card(
            id=self._get_next_id(),
            account_id=account.id,
            customer_id=account.customer_id,
            issued_at=datetime.now(tz=UTC),
        )
        self._insert(card)
        return card
