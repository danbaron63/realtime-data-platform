import random
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime

from generator.base import BaseDatabase, BaseEntity

os = [
    "android",
    "ios",
]


@dataclass
class Device(BaseEntity):
    id: str
    customer_id: str
    first_seen_at: datetime
    os: str

    @classmethod
    def get_constraints(cls) -> Iterable[str]:
        return [
            "FOREIGN KEY (customer_id) REFERENCES customer(id)",
        ]


@dataclass
class LoginAttempt(BaseEntity):
    id: str
    customer_id: str
    device_id: str
    timestamp: datetime
    success: bool
    ip_address: str
    country: str


class DeviceDatabase(BaseDatabase):
    def __init__(self, customer_db):
        super().__init__(Device)
        self._customer_db = customer_db

    def device_registered(self) -> Device:
        customer = self._customer_db.get_random()

        device = Device(
            id=self._get_next_id(),
            customer_id=customer.id,
            first_seen_at=datetime.now(tz=UTC),
            os=random.choice(os),
        )

        self._insert(device)
        return device

    def login_attempted(self) -> LoginAttempt:
        device = self.get_random()
        return LoginAttempt(
            id=self._get_next_id(),
            customer_id=device.customer_id,
            device_id=device.id,
            timestamp=datetime.now(tz=UTC),
            success=random.choice([True, False]),
            ip_address=self._faker.ipv4(),
            country=self._faker.country(),
        )
