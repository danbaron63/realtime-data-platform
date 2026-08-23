import random
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import uuid4

from generator.base import BaseDatabase, BaseEntity


class TransferType(StrEnum):
    INTERNAL = "INTERNAL"
    EXTERNAL_OUT = "EXTERNAL_OUT"
    EXTERNAL_IN = "EXTERNAL_IN"


@dataclass
class Transfer(BaseEntity):
    id: str
    timestamp: datetime
    source_account_id: str
    destination_account_id: str
    amount: float
    currency: str
    transfer_type: str


class TransferDatabase(BaseDatabase):
    def __init__(self, account_db):
        super().__init__(Transfer)
        self._account_db = account_db
        self._transfer_types = [
            TransferType.INTERNAL,
            TransferType.EXTERNAL_OUT,
            TransferType.EXTERNAL_IN,
        ]

    def transfer_completed(self) -> Transfer:
        transfer_type = random.choice(self._transfer_types)
        account_1 = self._account_db.get_random()

        match transfer_type:
            case TransferType.INTERNAL:
                account_2 = self._account_db.get_random(except_for=account_1.id)
                transfer = Transfer(
                    id=self._get_next_id(),
                    timestamp=datetime.now(tz=UTC),
                    source_account_id=str(account_1.id),
                    destination_account_id=str(account_2.id),
                    amount=random.randint(1, 10000000) / 100,
                    transfer_type=transfer_type,
                    currency=account_1.currency,
                )
            case TransferType.EXTERNAL_IN:
                transfer = Transfer(
                    id=self._get_next_id(),
                    timestamp=datetime.now(tz=UTC),
                    source_account_id=str(uuid4()),
                    destination_account_id=str(account_1.id),
                    amount=random.randint(1, 10000000) / 100,
                    transfer_type=transfer_type,
                    currency=account_1.currency,
                )
            case _:
                transfer = Transfer(
                    id=self._get_next_id(),
                    timestamp=datetime.now(tz=UTC),
                    source_account_id=str(account_1.id),
                    destination_account_id=str(uuid4()),
                    amount=random.randint(1, 10000000) / 100,
                    transfer_type=transfer_type,
                    currency=account_1.currency,
                )
        self._insert(transfer)
        return transfer
