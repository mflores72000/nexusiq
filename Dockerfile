FROM python:3.11-slim

WORKDIR /app

# Copiar e instalar dependencias Python primero (caching de capas)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente
COPY src/ ./src/
COPY data/ ./data/
COPY tests/ ./tests/

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
