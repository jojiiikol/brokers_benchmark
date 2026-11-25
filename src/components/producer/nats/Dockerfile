FROM python:3.14-slim

WORKDIR /app

COPY components/producer/nats/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY logger/config.py ./src/logger/config.py
COPY components/common/load_profile.py ./src/components/common/load_profile.py
COPY components/common/phase_control.py ./src/components/common/phase_control.py
COPY components/common/runtime.py ./src/components/common/runtime.py
COPY components/common/producer_metrics.py ./src/components/common/producer_metrics.py
COPY components/common/prometheus_metrics.py ./src/components/common/prometheus_metrics.py
COPY components/common/message_payload.py ./src/components/common/message_payload.py
COPY components/producer/nats/app.py ./app.py

EXPOSE 8000

ENTRYPOINT ["python3", "app.py"]
