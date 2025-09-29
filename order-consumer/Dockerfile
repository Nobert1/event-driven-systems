FROM python:3.12-slim


WORKDIR /app

# Install service dependencies
COPY order-consumer/consumer.py .
COPY order-consumer/requirements.txt .
RUN pip install -r requirements.txt
COPY . .

CMD ["python", "consumer.py"]
