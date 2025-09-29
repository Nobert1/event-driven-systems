FROM python:3.12-slim

WORKDIR /app

COPY order-service/app.py .
COPY order-service/requirements.txt .
RUN pip install -r requirements.txt
COPY . .

EXPOSE 8000