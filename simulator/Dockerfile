FROM python:3.11-slim

WORKDIR /app
ENV PYTHONPATH=/app

RUN pip install redis faker

COPY simulator/data_simulator.py ./simulator/
COPY config ./config

CMD ["python", "simulator/data_simulator.py"]
