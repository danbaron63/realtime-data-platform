from dataclasses import dataclass
from generator.customer import CustomerDatabase, Customer
from generator.base import BaseDatabase, BaseEntity
from datetime import date
from typing import Iterable
import random


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

    def get_accounts_for_customer(self, customer: Customer):
        return [a for a in self._database if a.customer_id == customer.id]

    def get_random_account_for_customer(self, customer: Customer) -> Account:
        accounts = self.get_accounts_for_customer(customer)
        if len(accounts) == 0:
            raise Exception(f"No accounts found for customer {customer.id}")
        return random.choice(accounts)

    def account_opened(self):
        # get a customer
        customer = self._customer_database.get_random()

        # get accounts for this customer
        if len(self.get_accounts_for_customer(customer)) > max_accounts_per_customer:
            return self.account_opened()

        account = Account(
            id=self._get_next_id(),
            customer_id=customer.id,
            account_type=random.choice(account_types),
            currency=random.choice(currencies),
            opened_at=date.today(),
        )
        self._insert(account)
        return account
