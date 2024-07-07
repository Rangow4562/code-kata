# Dockerfile

FROM python:3.8-slim

WORKDIR /app

COPY . /

RUN pip install --no-cache-dir -r /app/requirements.txt

ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]