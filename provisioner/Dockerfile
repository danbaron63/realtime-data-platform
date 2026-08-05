FROM python:3.12-slim

WORKDIR /app

RUN pip install --upgrade pip

COPY requirements.txt .
RUN --mount=type=cache,id=pip-cache,target=/root/.cache/pip \
    pip install -r requirements.txt

COPY provision_catalog.py .
COPY provision_pinot.py .
COPY pinot_config ./pinot_config

ENTRYPOINT ["python3"]
CMD ["/app/provision_catalog.py"]
