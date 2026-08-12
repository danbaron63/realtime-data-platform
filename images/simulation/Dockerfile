FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN --mount=type=cache,id=pip-cache,target=/root/.cache/pip \
    pip install -r requirements.txt

COPY simulate_consumer.py .
COPY freshness_probe.py .

ENTRYPOINT ["python3"]
CMD ["/app/simulate_consumer.py"]
