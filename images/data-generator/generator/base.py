import logging
import random
from abc import ABC
from collections.abc import Iterable
from dataclasses import dataclass, fields
from datetime import date, datetime
from enum import StrEnum

from dataclasses_avroschema import AvroModel
from faker import Faker

from generator.persistence import Persistence

logger = logging.getLogger(__name__)

SQL_LITE_TYPE_MAP = {
    str: "TEXT",
    int: "INTEGER",
    float: "REAL",
    datetime: "DATETIME",
    date: "DATE",
    StrEnum: "TEXT",
}


class EmptyDatabaseException(Exception):
    pass


@dataclass
class BaseEntity(AvroModel, ABC):
    id: str

    @classmethod
    def get_constraints(cls) -> Iterable[str]:
        return []

    class Meta:
        namespace = "raw"


class BaseDatabase(ABC):
    def __init__(self, entity_type: type[BaseEntity]):
        self._faker = Faker()
        self._persistence = Persistence()
        self._entity_type = entity_type
        self._columns = [f.name for f in fields(entity_type)]
        self._create_table()
        self._load_ids()
        self._id_counter = max(int(i) for i in self._ids) if self._ids else 0
        self._get_item_query = f"""
        SELECT {", ".join(self._columns)}
        FROM {self._entity_type.__name__}
        WHERE id = ?
        """
        self._insert_dml = f"""
        INSERT INTO {self._entity_type.__name__} ({",".join(self._columns)}) VALUES ({",".join(["?" for _ in self._columns])})
        """
        self._update_persistence_dml = f"""
        UPDATE {self._entity_type.__name__} SET {",".join([f"{c} = ?" for c in self._columns])} WHERE id = ?
        """
        logger.info(f"Initialised {self._entity_type} with {self._id_counter=}")

    def _get_next_id(self) -> str:
        self._id_counter += 1
        return str(self._id_counter)

    def get_random(self, except_for: str | None = None) -> object:
        """
        Get a random event from the database.

        :param except_for: optionally specify an element not to match.
        :return: a random event.
        """
        if len(self._ids) == 0:
            raise EmptyDatabaseException(f"{self._entity_type.__name__} is empty")
        _id = random.choice(self._ids)
        if _id == except_for:
            return self.get_random()
        return self.get_item(_id)

    def _insert(self, item: BaseEntity):
        item_dict = item.asdict()
        values = [item_dict[f] for f in self._columns]
        self._persistence.execute(self._insert_dml, values)
        self._ids.append(item.id)

    def get_item(self, _id: str) -> BaseEntity:
        result = self._persistence.query(self._get_item_query, [_id])
        records = [
            self._to_dataclass(dict(zip(self._columns, r)), fields(self._entity_type))
            for r in result
        ]
        if len(records) != 1:
            raise ValueError(f"multiple records returned: {records}")
        return records[0]

    def _create_table(self):
        logger.info(f"Creating table {self._entity_type.__name__}")
        columns = [
            f"{f.name} {SQL_LITE_TYPE_MAP[f.type]}" for f in fields(self._entity_type)
        ]
        columns.extend(self._entity_type.get_constraints())

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self._entity_type.__name__} (
        {",\n".join(columns)}
        )
        """
        self._persistence.ddl(ddl)

    def _update_persistence(self, item: BaseEntity):
        item_dict = item.asdict()
        values = [item_dict[f] for f in self._columns]
        self._persistence.execute(self._update_persistence_dml, [*values, item.id])

    def _to_dataclass(self, record, dataclass_fields):
        model_dict = {}
        for field in dataclass_fields:
            val = record[field.name]
            if field.type == datetime:
                val = datetime.fromisoformat(val)
            model_dict[field.name] = val
        return self._entity_type(**model_dict)

    def _load_ids(self):
        query = f"SELECT id FROM {self._entity_type.__name__}"
        records = self._persistence.query(query)
        self._ids = [r[0] for r in records]
