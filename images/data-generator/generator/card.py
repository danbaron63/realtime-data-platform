from dataclasses import dataclass
from datetime import UTC, datetime

from generator.base import BaseDatabase, BaseEntity

MAX_CARDS_PER_ACCOUNT = 2


@dataclass
class Card(BaseEntity):
    id: str
    account_id: str
    customer_id: str
    issued_at: datetime


class CardDatabase(BaseDatabase):
    def __init__(self, account_db):
        super().__init__(Card)
        self._account_db = account_db
        self._card_count_query = (
            f"SELECT count(*) FROM {self._entity_type.__name__} WHERE account_id = ?"
        )

    def card_issued(self) -> Card:
        # get customer
        account = self._account_db.get_random()

        # get cards for this account
        result = self._persistence.query(self._card_count_query, [account.id])
        no_of_cards = result[0][0]

        if no_of_cards >= MAX_CARDS_PER_ACCOUNT:
            # fetch a different one...
            return self.card_issued()

        card = Card(
            id=self._get_next_id(),
            account_id=account.id,
            customer_id=account.customer_id,
            issued_at=datetime.now(tz=UTC),
        )
        self._insert(card)
        return card
