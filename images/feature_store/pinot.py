import logging

import typing

import httpx
from pydantic import JsonValue

logger = logging.getLogger(__name__)


class PinotResponse(typing.NamedTuple):
    rows: list[dict[str, typing.Any]]
    metrics: dict[str, typing.Any]


class PinotQueryError(Exception):
    pass


class PinotClient:
    def __init__(self, base_url: str, timeout: float = 5.0):
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
        )

    async def close(self):
        logger.info("Closing httpx client for pinot")
        await self._client.aclose()

    async def query(
        self,
        sql: str,
        multi_stage: bool,
        parameters: dict[str, JsonValue] | None = None,
    ) -> PinotResponse:
        formatted_query = sql.format(**parameters)
        endpoint = "/query" if multi_stage else "/query/sql"

        response = await self._client.post(
            endpoint,
            json={
                "sql": formatted_query,
            },
        )

        response.raise_for_status()

        payload = response.json()

        if payload.get("exceptions", []):
            raise PinotQueryError(payload["exceptions"])

        result = payload["resultTable"]
        columns = result["dataSchema"]["columnNames"]
        rows = result["rows"]

        records = [dict(zip(columns, row)) for row in rows]
        return PinotResponse(
            rows=records,
            metrics={
                "timeUsedMs": payload["timeUsedMs"],
            }
        )
