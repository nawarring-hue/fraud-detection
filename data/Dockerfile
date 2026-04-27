FROM python:3.11-slim

WORKDIR /app
ENV PYTHONPATH=/app

RUN pip install redis

COPY data/initial_profiles.py ./data/
COPY config ./config

CMD ["python", "data/initial_profiles.py"]
