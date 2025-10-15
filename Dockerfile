FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/
RUN apt-get update && \
    apt-get install -y build-essential libpq-dev curl && \
    pip install --upgrade pip && \
    pip install -r requirements.txt && \
    python -m spacy download en_core_web_sm && \
    rm -rf /var/lib/apt/lists/*

# install playwright browsers
RUN pip install playwright && \
    python -m playwright install --with-deps chromium

COPY src /app/src
WORKDIR /app/src
ENV PYTHONUNBUFFERED=1
CMD ["python", "main.py"]
