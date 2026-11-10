FROM python:3.14-slim

WORKDIR /app

COPY components/producer/kafka/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY logger/config.py ./src/logger/config.py
COPY components/common/load_profile.py ./src/components/common/load_profile.py
COPY components/producer/kafka/app.py ./app.py

ENTRYPOINT ["python3", "app.py"]
