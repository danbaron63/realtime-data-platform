# Feature Store

This package contains a simple Python, FastAPI feature store implementation that uses Apache Pinot
as its storage backend and query processing layer.

## Configuration

Features can be configured in the feature store in the [config directory](feature_config).
The configuration currently supports two concepts:
1. features
2. feature sets

### Features

Features are a collection of feature values produced by a single query. 
They consist of one or more entity keys and contain one or more feature values - aggregations.

Features are configured by providing SQL code which calculates the features in Pinot and a list of entity columns.
You can configure arbitrary SQL to run against Pinot making this architecture incredibly flexible whilst still 
supporting real time online store use cases.

### Feature Sets

Feature sets are a collection of _Features_ that share one or more entity keys.
This allows them to be joined into a single record.

## Implementation

The server is implemented using FastAPI and leverages `asyncio` for improved performance.
Interactions with Apache Pinot are handled by `pinotdb` and async methods are used where possible.
