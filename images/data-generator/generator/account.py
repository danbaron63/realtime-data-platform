import random
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime

from generator.base import BaseDatabase, BaseEntity
from generator.customer import CustomerDatabase

max_accounts_per_customer = 10
account_types = [
    "current",
    "credit",
    "savings",
]
currencies = [
    "GBP",
    "USD",
    "EUR",
]


@dataclass
class Account(BaseEntity):
    id: str
    customer_id: str
    account_type: str
    currency: str
    opened_at: date

    @classmethod
    def get_constraints(cls) -> Iterable[str]:
        return ["FOREIGN KEY (customer_id) REFERENCES customer(id)"]


class AccountDatabase(BaseDatabase):
    def __init__(self, customer_database: CustomerDatabase):
        super().__init__(Account)
        self._customer_database = customer_database
        self._account_count_query = (
            f"SELECT count(*) FROM {self._entity_type.__name__} WHERE customer_id = ?"
        )

    def account_opened(self):
        # get a customer
        customer = self._customer_database.get_random()

        # get accounts for this customer
        result = self._persistence.query(self._account_count_query, [customer.id])
        no_of_accounts = result[0][0]

        if no_of_accounts >= max_accounts_per_customer:
            return self.account_opened()

        account = Account(
            id=self._get_next_id(),
            customer_id=customer.id,
            account_type=random.choice(account_types),
            currency=random.choice(currencies),
            opened_at=datetime.now(tz=UTC).date(),
        )
        self._insert(account)
        return account
