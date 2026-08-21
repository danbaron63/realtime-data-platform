import asyncio
import logging
import os
from contextlib import asynccontextmanager
from itertools import chain

from anyio import open_file
from fastapi import FastAPI, HTTPException
from pinot import PinotClient, PinotConnectionError
from prometheus_client import Histogram
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, JsonValue
from yaml import safe_load


@asynccontextmanager
async def lifespan(app: FastAPI):
    config_path = os.environ["CONFIG_PATH"]
    async with await open_file(config_path) as f:
        config = safe_load(await f.read())
    app.state.feature_config = Config(**config)
    app.state.pinot = PinotClient(
        base_url=os.environ["PINOT_BROKER"],
    )
    app.state.pinot_time_used_histogram = Histogram(
        "pinot_time_used_ms",
        "Time used by pinot to generate response",
        labelnames=["feature"],
        buckets=(
            1,
            2,
            3,
            5,
            8,
            12,
            16,
            20,
            25,
            30,
            40,
            50,
            70,
            90,
            120,
            150,
            200,
            250,
            300,
        ),
    )

    yield

    await app.state.pinot.close()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
app = FastAPI(lifespan=lifespan)
Instrumentator().instrument(app).expose(app)


class Feature(BaseModel):
    sql: str
    entities: list[str]
    multi_stage_engine: bool = False


class FeatureSet(BaseModel):
    features: list[str]


class Config(BaseModel):
    features: dict[str, Feature]
    feature_sets: dict[str, FeatureSet]


class Entity(BaseModel):
    entity_keys: dict[str, JsonValue]


@app.post("/features/{feature}")
async def get_features(feature: str, item: Entity):
    if feature not in app.state.feature_config.features:
        raise HTTPException(status_code=404, detail=f"Feature {feature} not found")
    conf = app.state.feature_config.features[feature]
    for entity in conf.entities:
        if entity not in item.entity_keys:
            raise HTTPException(
                status_code=400, detail=f"Entity {entity} required in the request"
            )

    sql = conf.sql
    use_multi_stage = conf.multi_stage_engine

    try:
        pinot_response = await app.state.pinot.query(
            sql, use_multi_stage, parameters=item.entity_keys
        )
    except PinotConnectionError as e:
        logger.error(f"returning HTTP 500: {e!s}")
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )

    rows = pinot_response.rows
    if len(rows) > 1:
        logger.error(f"returning HTTP 500: {feature} with entity keys: {item.entity_keys} returned {len(rows)}: {rows}")
        raise HTTPException(
            status_code=500,
            detail=(
                f"Feature '{feature}' returned {len(rows)} rows; expected at most one"
            ),
        )
    if len(rows) == 0:
        logger.error(f"returning HTTP 404: {feature} with entity keys: {item.entity_keys} returned 0 records")
        raise HTTPException(
            status_code=404, detail=f"Feature '{feature}' returned no rows"
        )

    app.state.pinot_time_used_histogram.labels(feature=feature).observe(pinot_response.metrics["timeUsedMs"])
    return rows[0]


@app.get("/config")
async def get_config():
    return app.state.feature_config


@app.post("/feature-set/{feature_set}")
async def get_feature_set(feature_set: str, item: Entity):
    if feature_set not in app.state.feature_config.feature_sets:
        raise HTTPException(status_code=404, detail=f"Feature {feature_set} not found")
    features = app.state.feature_config.feature_sets[feature_set].features
    entities = list(
        chain.from_iterable([app.state.feature_config.features[f].entities for f in features])
    )
    async with asyncio.TaskGroup() as tg:
        feature_tasks = {f: tg.create_task(get_features(f, item)) for f in features}
    feature_results = {f: t.result() for f, t in feature_tasks.items()}
    result = {}

    for feature_name, feature_dict in feature_results.items():
        for feature_column, feature_value in feature_dict.items():
            if feature_column in entities:
                result[feature_column] = feature_value
            else:
                result[f"{feature_name}.{feature_column}"] = feature_value

    return result
