FROM python:3.12-slim


WORKDIR /app

# Install service dependencies
COPY stock-consumer/consumer.py .
COPY stock-consumer/requirements.txt .
RUN pip install -r requirements.txt
COPY . .

CMD ["python", "consumer.py"]
