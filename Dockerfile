FROM python:3.11-slim

RUN pip install uv

WORKDIR /app

COPY requirements.txt .
RUN uv pip install -r requirements.txt

COPY . .
