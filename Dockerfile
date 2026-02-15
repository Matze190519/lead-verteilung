FROM python:3.11-slim

WORKDIR /app

# Dependencies installieren
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App-Code kopieren
COPY app.py .

# Port aus Environment-Variable (Render/Railway setzen das automatisch)
ENV PORT=8000

# Server starten
CMD uvicorn app:app --host 0.0.0.0 --port $PORT --log-level info
