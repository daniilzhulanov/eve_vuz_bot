FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Устанавливаем системные зависимости для pandas и openpyxl
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Устанавливаем Python-зависимости
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install openpyxl==3.1.2 beautifulsoup4 --force-reinstall

COPY . .

# Проверяем установку openpyxl
RUN python -c "import openpyxl; print(f'Openpyxl version: {openpyxl.__version__}')"

CMD ["python", "main.py"]
