import asyncio
import logging
import typing

import aiohttp
import msgspec
from pydantic import JsonValue

logger = logging.getLogger(__name__)

class PinotMetrics(msgspec.Struct):
    timeUsedMs: int


class PinotResponse(msgspec.Struct):
    rows: list[dict[str, typing.Any]]
    metrics: PinotMetrics


class DataSchema(msgspec.Struct):
    columnNames: list[str]


class ResultTable(msgspec.Struct):
    dataSchema: DataSchema
    rows: list[list[typing.Any]]


class PinotPayload(msgspec.Struct):
    timeUsedMs: int
    resultTable: ResultTable
    exceptions: list[dict[str, typing.Any]] = msgspec.field(default_factory=list)


class PinotQueryRequest(msgspec.Struct):
    sql: str


class PinotException(Exception):
    pass


class PinotQueryError(PinotException):
    pass


class PinotConnectionError(PinotException):
    pass


class PinotClient:
    def __init__(self, base_url: str, timeout: float = 5.0):
        self._client = aiohttp.ClientSession(
            base_url=base_url,
            timeout=aiohttp.ClientTimeout(total=timeout),
        )
        self._encoder = msgspec.json.Encoder()
        self._pinot_payload_decoder = msgspec.json.Decoder(PinotPayload)
        self._json_headers = {
            "Content-Type": "application/json",
        }

    async def close(self):
        logger.info("Closing http client for pinot")
        await self._client.close()

    async def query(
        self,
        sql: str,
        multi_stage: bool,
        parameters: dict[str, JsonValue],
    ) -> PinotResponse:
        formatted_query = sql.format(**parameters)

        endpoint = "/query" if multi_stage else "/query/sql"
        body = self._encoder.encode(PinotQueryRequest(sql=formatted_query))

        try:
            async with self._client.post(
                endpoint,
                data=body,
                headers=self._json_headers,
            ) as response:
                response_bytes = await response.read()
                try:
                    response.raise_for_status()
                except aiohttp.ClientResponseError as e:
                    raise PinotConnectionError(
                        f"Unsuccessful response from Pinot ({response.status}): {response_bytes.decode("utf-8")}") from e

                payload: PinotPayload = self._pinot_payload_decoder.decode(response_bytes)

        except asyncio.TimeoutError as te:
            raise PinotConnectionError("Pinot connection timed out") from te
        except aiohttp.ClientError as ce:
            raise PinotConnectionError("Failed to communicate with Pinot") from ce

        if payload.exceptions:
            raise PinotQueryError(f"Pinot responded with exceptions: {payload.exceptions}")

        columns = payload.resultTable.dataSchema.columnNames
        rows = payload.resultTable.rows

        records = [dict(zip(columns, row)) for row in rows]
        return PinotResponse(
            rows=records,
            metrics=PinotMetrics(
                timeUsedMs=payload.timeUsedMs,
            ),
        )
