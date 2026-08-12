from dataclasses import dataclass
from generator.base import BaseDatabase, BaseEntity
from datetime import datetime
from enum import StrEnum
from uuid import uuid4
import random


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

    def transfer_completed(self) -> Transfer | None:
        transfer_type = random.choice(
            [TransferType.INTERNAL, TransferType.EXTERNAL_OUT, TransferType.EXTERNAL_IN]
        )

        account_1 = self._account_db.get_random()

        match transfer_type:
            case TransferType.INTERNAL:
                account_2 = self._account_db.get_random()
                if account_1.id == account_2.id:
                    return None
                transfer = Transfer(
                    id=self._get_next_id(),
                    timestamp=datetime.now(),
                    source_account_id=str(account_1.id),
                    destination_account_id=str(account_2.id),
                    amount=random.randint(1, 10000000) / 100,
                    transfer_type=transfer_type,
                    currency=account_1.currency,
                )
            case TransferType.EXTERNAL_IN:
                transfer = Transfer(
                    id=self._get_next_id(),
                    timestamp=datetime.now(),
                    source_account_id=str(uuid4()),
                    destination_account_id=str(account_1.id),
                    amount=random.randint(1, 10000000) / 100,
                    transfer_type=transfer_type,
                    currency=account_1.currency,
                )
            case TransferType.EXTERNAL_OUT:
                transfer = Transfer(
                    id=self._get_next_id(),
                    timestamp=datetime.now(),
                    source_account_id=str(account_1.id),
                    destination_account_id=str(uuid4()),
                    amount=random.randint(1, 10000000) / 100,
                    transfer_type=transfer_type,
                    currency=account_1.currency,
                )
        self._insert(transfer)
        return transfer
