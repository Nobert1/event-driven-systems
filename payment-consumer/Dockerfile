FROM python:3.12-slim


WORKDIR /app

# Install service dependencies
COPY payment-consumer/consumer.py .
COPY payment-consumer/requirements.txt .
RUN pip install -r requirements.txt
COPY . .

CMD ["python", "consumer.py"]
