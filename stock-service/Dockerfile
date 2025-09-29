FROM python:3.12-slim

WORKDIR /app

COPY stock-service/app.py .
COPY stock-service/requirements.txt .
RUN pip install -r requirements.txt
COPY . .

EXPOSE 8000