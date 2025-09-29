FROM python:3.12-slim

WORKDIR /app

COPY payment-service/app.py .
COPY payment-service/requirements.txt .
RUN pip install -r requirements.txt
COPY . .

EXPOSE 8000