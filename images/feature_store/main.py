import asyncio
from itertools import chain
from fastapi import FastAPI, HTTPException
from pinotdb import connect_async
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel, JsonValue
from yaml import safe_load
import os
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
app = FastAPI()
Instrumentator().instrument(app).expose(app)
config_path = os.environ["CONFIG_PATH"]


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


with open(config_path) as f:
    config = safe_load(f.read())
feature_config = Config(**config)


@app.post("/features/{feature}")
async def get_features(feature: str, item: Entity):
    # There's a bug in pinotdb in which despite calling curs.close(), the cursor is retained.
    #  For now the only way around this is to create a new connection per request.
    conn = connect_async(host=os.environ["PINOT_BROKER"], port=8099, path="/query/sql", scheme="http")
    if feature not in feature_config.features:
        raise HTTPException(status_code=404, detail=f"Feature {feature} not found")
    conf = feature_config.features[feature]
    for entity in conf.entities:
        if entity not in item.entity_keys:
            raise HTTPException(status_code=400, detail=f"Entity {entity} required in the request")

    sql = conf.sql
    logger.debug(f"{feature}: executing with sql: {sql}")

    curs = conn.cursor()
    try:
        await curs.execute(
            sql,
            item.entity_keys,
            queryOptions=f"useMultistageEngine={str(conf.multi_stage_engine).lower()}"
        )
        columns = [desc[0] for desc in curs.description]
        record = curs.fetchone()
        if not record:
            return {col: None for col in columns}
        return dict(zip(columns, record))
    finally:
        await curs.close()
        await conn.close()


@app.get("/config")
async def get_config():
    return feature_config


@app.post("/feature-set/{feature_set}")
async def get_feature_set(feature_set: str, item: Entity):
    if feature_set not in feature_config.features:
        raise HTTPException(status_code=404, detail=f"Feature {feature_set} not found")
    features = feature_config.feature_sets[feature_set].features
    entities = list(chain.from_iterable([feature_config[f].entities for f in features]))
    async with asyncio.TaskGroup() as tg:
        feature_tasks = {
            f: tg.create_task(get_features(f, item)) for f in features
        }
    feature_results = {f: t.result() for f, t in feature_tasks.items()}
    result = dict()

    for feature_name, feature_dict in feature_results.items():
        for feature_column, feature_value in feature_dict.items():
            if feature_column in entities:
                result[feature_column] = feature_value
            else:
                result[f"{feature_name}.{feature_column}"] = feature_value

    return result