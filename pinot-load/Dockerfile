FROM python:3.12-slim

WORKDIR /app

RUN pip install --upgrade pip

COPY requirements.txt .
RUN --mount=type=cache,id=pip-cache,target=/root/.cache/pip \
    pip install -r requirements.txt

COPY extract.py .
COPY schemas ./schemas

ENV SCHEMAS_DIR="/app/schemas"

ENTRYPOINT ["python3"]
