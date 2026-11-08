FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir confluent-kafka==2.5.3 clickhouse-connect==0.8.3
COPY consumer.py .

CMD ["python", "-u", "consumer.py"]
