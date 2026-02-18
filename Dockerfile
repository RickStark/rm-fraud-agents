FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema necesarias para PostgreSQL y otros
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    curl \
    netcat-openbsd \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements
COPY requirements.txt .

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
COPY . .

# El comando se especifica en docker-compose.yml
CMD ["tail", "-f", "/dev/null"]
