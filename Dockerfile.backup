# 1. Usamos una imagen oficial de Python ligera para optimizar el peso
FROM python:3.11-slim

# 2. Establecemos el directorio de trabajo dentro del contenedor
WORKDIR /app

# 3. Copiamos el archivo de requerimientos primero (aprovecha la caché de Docker)
COPY requirements.txt .

# 4. Instalamos las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copiamos el resto del código (main.py)
COPY src/ ./src/

# 6. Definimos variables de entorno útiles para producción
ENV PYTHONUNBUFFERED=1 

# 7. Comando para arrancar el sistema multi-agente
CMD ["python", "src/main.py"]