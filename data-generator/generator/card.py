from dataclasses import dataclass
from generator.base import BaseDatabase, BaseEntity
from datetime import datetime
import random


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

    def card_issued(self) -> Card | None:
        # get customer
        account = self._account_db.get_random()
        customer = self._customer_db.get_item(account.customer_id)

        card = Card(
            id=self._get_next_id(),
            account_id=account.id,
            customer_id=customer.id,
            issued_at=datetime.now(),
        )
        self._insert(card)
        return card

    def get_cards_for_account(self, account):
        return [c for c in self._database if c.account_id == account.id]

    def get_random_card_for_account(self, account):
        cards = self.get_cards_for_account(account)
        if len(cards) == 0:
            raise Exception(f"No cards found for account {account.id}")
        return random.choice(cards)
