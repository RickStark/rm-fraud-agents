#!/bin/bash

echo "🚀 Iniciando Sistema de Detección de Fraude con Kafka + PostgreSQL + CrewAI"
echo "============================================================================"
echo ""

# Colores
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Verificar si Docker está corriendo
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}❌ Error: Docker no está corriendo${NC}"
    echo "Por favor inicia Docker y vuelve a intentar"
    exit 1
fi

# Verificar si docker-compose está instalado
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ Error: docker-compose no está instalado${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Docker está corriendo${NC}"
echo ""

# Detener contenedores anteriores si existen
echo "🧹 Limpiando contenedores anteriores..."
docker-compose down 2>/dev/null

echo ""
echo -e "${YELLOW}📦 Levantando servicios (esto puede tomar unos minutos)...${NC}"
docker-compose up -d

echo ""
echo "⏳ Esperando a que los servicios estén listos..."
sleep 15

# Verificar que los contenedores estén corriendo
echo ""
echo "🔍 Verificando estado de los servicios:"
docker-compose ps

echo ""
echo -e "${YELLOW}📥 Descargando modelo Mistral en Ollama (esto puede tomar varios minutos)...${NC}"
docker exec ollama ollama pull mistral

echo ""
echo -e "${GREEN}============================================================================${NC}"
echo -e "${GREEN}✅ Sistema iniciado correctamente!${NC}"
echo -e "${GREEN}============================================================================${NC}"
echo ""
echo "📊 Servicios disponibles:"
echo ""
echo "  • Kafka UI:       http://localhost:8080"
echo "  • Adminer (DB):   http://localhost:8081"
echo "    - Usuario: postgres"
echo "    - Password: postgres"
echo "    - Base de datos: fraud_detection"
echo ""
echo "📝 Ver logs de los servicios:"
echo ""
echo "  docker logs -f fraud-detection-app    # App principal"
echo "  docker logs -f transaction-producer   # Generador de transacciones"
echo "  docker logs -f kafka                  # Kafka"
echo "  docker logs -f postgres-fraud-detection  # PostgreSQL"
echo ""
echo "🧪 Enviar transacción manual:"
echo ""
echo "  python send_transaction.py"
echo ""
echo "🛑 Detener todo:"
echo ""
echo "  docker-compose down"
echo ""
echo -e "${GREEN}¡Listo para detectar fraude! 🔍${NC}"
