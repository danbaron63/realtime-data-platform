import random
import logging
from faker import Faker
from abc import ABC
from dataclasses import dataclass, fields
from dataclasses_avroschema import AvroModel
from datetime import date, datetime
from enum import StrEnum
from generator.persistence import Persistence
from typing import Iterable


logger = logging.getLogger(__name__)

SQL_LITE_TYPE_MAP = {
    str: "TEXT",
    int: "INTEGER",
    float: "REAL",
    datetime: "DATETIME",
    date: "DATE",
    StrEnum: "TEXT",
}


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
        self._database = list()
        self._id_counter = 0
        self._persistence = Persistence()
        self._entity_type = entity_type
        self._create_table()
        self._load_records()

    def _get_next_id(self) -> str:
        self._id_counter += 1
        return str(self._id_counter)

    def get_random(self) -> object | None:
        if len(self._database) == 0:
            return None
        return random.choice(self._database)

    def _insert(self, item):
        self._insert_persistence(item)
        self._database.append(item)

    def get_item(self, i: str):
        matches = [e for e in self._database if e.id == i]
        if len(matches) == 0:
            raise Exception(f"Item with id {i} not found in {type(self).__name__}")
        return matches[0]

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

    def _insert_persistence(self, item: BaseEntity):
        item_dict = item.asdict()
        columns = [f.name for f in fields(item)]
        placeholders = ["?" for _ in columns]
        values = [item_dict[f] for f in columns]

        dml = f"""
        INSERT INTO {type(item).__name__} ({",".join(columns)}) VALUES ({",".join(placeholders)})
        """
        self._persistence.execute(dml, values)

    def _update_persistence(self, item: BaseEntity):
        item_dict = item.asdict()
        columns = [f.name for f in fields(item)]
        values = [item_dict[f] for f in columns]
        update = [f"{c} = ?" for c in columns]

        dml = f"""
        UPDATE {type(item).__name__} SET {",".join(update)} WHERE id = ?
        """
        self._persistence.execute(dml, [*values, item.id])

    def _load_records(self):
        def to_dataclass(record, dataclass_fields):
            model_dict = {}
            for field in dataclass_fields:
                val = record[field.name]
                if field.type == datetime:
                    val = datetime.fromisoformat(val)
                model_dict[field.name] = val
            return self._entity_type(**model_dict)

        cols = [f.name for f in fields(self._entity_type)]
        query = f"SELECT {', '.join(cols)} FROM {self._entity_type.__name__}"
        records = self._persistence.query(query)
        self._database = [
            to_dataclass(dict(zip(cols, r)), fields(self._entity_type)) for r in records
        ]
