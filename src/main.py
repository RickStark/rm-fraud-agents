import os
import warnings
import json
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaConsumer
from crewai import Agent, Task, Crew, Process, LLM


# Suprimir warning de TracerProvider
os.environ["OTEL_SDK_DISABLED"] = "true"
warnings.filterwarnings("ignore", message=".*TracerProvider.*")

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================
# Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fraud-detection-group")

# PostgreSQL
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "fraud_detection")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

# Ollama
OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")

print(f"🔌 Conectando al LLM Local en: {OLLAMA_URL}")
print(f"📊 Conectando a PostgreSQL: {DB_HOST}:{DB_PORT}/{DB_NAME}")
print(f"📨 Conectando a Kafka: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}")

# ==========================================
# 2. CONFIGURACIÓN DEL LLM
# ==========================================
llm = LLM(
    model="ollama/mistral",
    base_url=OLLAMA_URL,
    temperature=0
)

# ==========================================
# 3. DEFINICIÓN DE AGENTES
# ==========================================
fraud_analyst = Agent(
    role='Senior Fraud Analyst',
    goal='Analizar transacciones financieras y evaluar el riesgo.',
    backstory=(
        "Eres un analista experto de prevención de fraude bancario. "
        "Sigues estas reglas ESTRICTAS para evaluar riesgo:\n\n"
        "RIESGO ALTO (Score 80-100):\n"
        "- Transacción en país diferente al de residencia SIN aviso de viaje\n"
        "- Monto 7x o más que el ticket promedio\n"
        "- Combinación de: país diferente + comercio inusual + monto alto\n\n"
        "RIESGO MEDIO (Score 40-79):\n"
        "- Transacción en país diferente CON aviso de viaje\n"
        "- Monto entre 3x y 7x el ticket promedio en ubicación usual\n"
        "- Categoría de comercio nueva pero monto normal\n\n"
        "RIESGO BAJO (Score 0-39):\n"
        "- Transacción en ciudad/país de residencia\n"
        "- Monto menor o similar al ticket promedio\n"
        "- Categoría de comercio conocida\n\n"
        "IMPORTANTE: Compara CORRECTAMENTE las ubicaciones. 'Lima, Peru' y 'Lima, Perú' son LA MISMA ubicación. "
        "Un monto MENOR que el promedio NO es sospechoso. "
        "Sé preciso y objetivo en tu análisis."
    ),
    verbose=True,
    allow_delegation=False,
    llm=llm,
    tools=[]
)

decision_maker = Agent(
    role='Risk Decision Executor',
    goal='Tomar la decisión final basándose en el análisis y redactar el veredicto',
    backstory=(
        "Eres el sistema automatizado de cumplimiento. "
        "Tomas decisiones irrevocables basándote ÚNICAMENTE en las reglas de negocio."
    ),
    verbose=True,
    allow_delegation=False,
    llm=llm,
    tools=[]
)

# ==========================================
# 4. FUNCIONES DE BASE DE DATOS
# ==========================================
def get_db_connection():
    """Establece conexión con PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"❌ Error conectando a la base de datos: {e}")
        raise

def get_transaction_details(tx_id):
    """Obtiene los detalles de una transacción desde la BD"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT tx_id, user_id, amount, location, merchant, timestamp
                FROM transactions
                WHERE tx_id = %s
            """, (tx_id,))
            result = cursor.fetchone()
            return dict(result) if result else None
    finally:
        conn.close()

def get_user_historical_profile(user_id):
    """Obtiene el perfil histórico del usuario desde la BD"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Obtener perfil del usuario
            cursor.execute("""
                SELECT 
                    country_of_residence,
                    has_travel_notice
                FROM user_profiles
                WHERE user_id = %s
            """, (user_id,))
            profile = cursor.fetchone()
            
            if not profile:
                return None
            
            # Calcular promedio de los últimos 30 días
            cursor.execute("""
                SELECT 
                    AVG(amount) as avg_ticket,
                    ARRAY_AGG(DISTINCT merchant_category) as categories
                FROM transactions
                WHERE user_id = %s
                  AND timestamp >= NOW() - INTERVAL '30 days'
                  AND timestamp < (
                      SELECT timestamp FROM transactions WHERE tx_id = 
                      (SELECT tx_id FROM transactions WHERE user_id = %s ORDER BY timestamp DESC LIMIT 1)
                  )
            """, (user_id, user_id))
            stats = cursor.fetchone()
            
            return {
                "country_of_residence": profile["country_of_residence"],
                "avg_ticket_last_30_days": float(stats["avg_ticket"]) if stats["avg_ticket"] else 0.0,
                "frequent_categories": stats["categories"] if stats["categories"] else [],
                "has_travel_notice": profile["has_travel_notice"]
            }
    finally:
        conn.close()

def save_fraud_analysis_result(tx_id, risk_score, decision, analysis_text):
    """Guarda el resultado del análisis en la BD"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO fraud_analysis_results 
                (tx_id, risk_score, decision, analysis_text, analyzed_at)
                VALUES (%s, %s, %s, %s, NOW())
            """, (tx_id, risk_score, decision, analysis_text))
            conn.commit()
            print(f"✅ Resultado guardado en BD para TX: {tx_id}")
    finally:
        conn.close()

# ==========================================
# 5. PROCESAMIENTO DE TRANSACCIONES
# ==========================================
def process_transaction(tx_data, user_history):
    """Procesa una transacción con los agentes de CrewAI"""
    print(f"\n⚡ Iniciando análisis local para TX: {tx_data['tx_id']}...")
    
    analysis_task = Task(
        description=(
            f"Analiza esta transacción siguiendo el proceso paso a paso:\n\n"
            f"DATOS DE LA TRANSACCIÓN:\n"
            f"{json.dumps(tx_data, default=str, indent=2)}\n\n"
            f"HISTORIAL DEL USUARIO:\n"
            f"{json.dumps(user_history, indent=2)}\n\n"
            f"PROCESO DE ANÁLISIS:\n"
            f"1. Comparar ubicaciones:\n"
            f"   - Ubicación de transacción: {tx_data.get('location', 'N/A')}\n"
            f"   - País de residencia: {user_history.get('country_of_residence', 'N/A')}\n"
            f"   - ¿Es la misma ciudad/país? (Considera variaciones de escritura)\n\n"
            f"2. Comparar montos:\n"
            f"   - Monto actual: ${tx_data.get('amount', 0)}\n"
            f"   - Promedio últimos 30 días: ${user_history.get('avg_ticket_last_30_days', 0)}\n"
            f"   - Calcular ratio: monto_actual / promedio\n"
            f"   - ¿Es el monto MAYOR a 7x el promedio?\n\n"
            f"3. Verificar categoría de comercio:\n"
            f"   - Comercio: {tx_data.get('merchant', 'N/A')}\n"
            f"   - Categoría inferida (ej: supermercado = Groceries)\n"
            f"   - Categorías habituales: {user_history.get('frequent_categories', [])}\n"
            f"   - ¿Es una categoría conocida?\n\n"
            f"4. Verificar aviso de viaje:\n"
            f"   - Tiene aviso: {user_history.get('has_travel_notice', False)}\n\n"
            f"5. Calcular score de riesgo (0-100) basado en:\n"
            f"   - ALTO (80-100): Ubicación diferente SIN aviso + monto >7x promedio\n"
            f"   - MEDIO (40-79): Ubicación diferente CON aviso O monto 3-7x promedio\n"
            f"   - BAJO (0-39): Misma ubicación + monto normal + categoría conocida\n\n"
            f"Proporciona el score numérico al final."
        ),
        expected_output=(
            "Un reporte en español con:\n"
            "1. Comparación clara de ubicaciones (¿misma ciudad o diferente?)\n"
            "2. Ratio de monto (cuántas veces el promedio)\n"
            "3. Categoría del comercio y si es habitual\n"
            "4. Justificación del score\n"
            "5. Score de riesgo (número del 0 al 100)"
        ),
        agent=fraud_analyst
    )

    decision_task = Task(
        description=(
            "Lee CUIDADOSAMENTE el análisis de riesgo previo.\n\n"
            "Extrae el score de riesgo que calculó el analista.\n\n"
            "REGLAS DE DECISIÓN (NO NEGOCIABLES):\n"
            "- Score >= 80 → Decisión: BLOCK\n"
            "- Score entre 40 y 79 → Decisión: FLAG\n"
            "- Score < 40 → Decisión: APPROVE\n\n"
            "FORMATO DE RESPUESTA (exacto):\n"
            "Decisión: [BLOCK/FLAG/APPROVE]\n"
            "Justificación: [1-2 líneas explicando por qué]\n\n"
            "NO inventes un score nuevo. Usa el del análisis previo."
        ),
        expected_output=(
            "Una respuesta en este formato exacto:\n"
            "Decisión: [BLOCK/FLAG/APPROVE]\n"
            "Justificación: [breve explicación]"
        ),
        agent=decision_maker
    )

    fraud_crew = Crew(
        agents=[fraud_analyst, decision_maker],
        tasks=[analysis_task, decision_task],
        process=Process.sequential,
        verbose=True
    )

    start_time = time.time()
    result = fraud_crew.kickoff()
    elapsed_time = time.time() - start_time
    
    print(f"\n✅ Tiempo de ejecución local: {elapsed_time:.2f}s")
    print(f"Resultado:\n{result}")
    
    # Extraer decisión y score (parsing básico)
    result_str = str(result)
    decision = "FLAG"  # Default
    risk_score = 50  # Default
    
    if "BLOCK" in result_str.upper():
        decision = "BLOCK"
        risk_score = 85
    elif "APPROVE" in result_str.upper():
        decision = "APPROVE"
        risk_score = 30
    
    # Guardar resultado en BD
    save_fraud_analysis_result(tx_data['tx_id'], risk_score, decision, result_str)
    
    return result

# ==========================================
# 6. CONSUMIDOR DE KAFKA
# ==========================================
def consume_transactions():
    """Consume mensajes de Kafka y procesa transacciones"""
    print(f"\n🎧 Iniciando consumidor de Kafka...")
    print(f"   Broker: {KAFKA_BROKER}")
    print(f"   Topic: {KAFKA_TOPIC}")
    print(f"   Group ID: {KAFKA_GROUP_ID}\n")
    
    # Esperar a que Kafka esté disponible
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                # --- Timeouts aumentados para GPU lenta (GTX 1650) ---
                # El LLM puede tardar varios minutos en responder
                session_timeout_ms=600000,      # 10 minutos (default: 10s)
                heartbeat_interval_ms=60000,    # Heartbeat cada 1 min (default: 3s)
                max_poll_interval_ms=600000,    # Maximo entre polls: 10 min
                request_timeout_ms=660000,      # Debe ser > session_timeout
                # --- Un mensaje a la vez para no saturar la GPU ---
                max_poll_records=1,
            )
            print("✅ Conectado a Kafka exitosamente!")
            break
        except Exception as e:
            retry_count += 1
            print(f"⏳ Esperando a Kafka... Intento {retry_count}/{max_retries}")
            time.sleep(5)
    
    if retry_count >= max_retries:
        print("❌ No se pudo conectar a Kafka después de varios intentos")
        return
    
    print("👀 Esperando mensajes...\n")
    # Constantes configurables
    AMOUNT_THRESHOLD = 300
    try:
        for message in consumer:
            try:
                # El mensaje de Kafka contiene el tx_id
                kafka_data = message.value
                tx_id = kafka_data.get('tx_id')
                
                print(f"\n📨 Mensaje recibido de Kafka: {kafka_data}")
                
                # Obtener datos completos de la transacción desde BD
                tx_data = get_transaction_details(tx_id)
                if not tx_data:
                    print(f"⚠️  Transacción {tx_id} no encontrada en la base de datos")
                    continue
                
                # Obtener perfil histórico del usuario
                user_history = get_user_historical_profile(tx_data['user_id'])
                if not user_history:
                    print(f"⚠️  Perfil de usuario {tx_data['user_id']} no encontrado")
                    continue
                
                # Procesar según el monto
                amount = float(tx_data['amount'])
                if amount > AMOUNT_THRESHOLD:
                    process_transaction(tx_data, user_history)
                    print(f"✅ Transacción {tx_id} procesada (monto: {amount})")
                else:
                    print(f"ℹ️  Transacción {tx_id} omitida - monto bajo: {amount}")
                
            except Exception as e:
                print(f"❌ Error procesando mensaje: {e}")
                continue
                
    except KeyboardInterrupt:
        print("\n👋 Deteniendo consumidor...")
    finally:
        consumer.close()

# ==========================================
# 7. PUNTO DE ENTRADA
# ==========================================
if __name__ == "__main__":
    # Esperar un poco para que los servicios estén listos
    print("⏳ Esperando a que los servicios estén listos...")
    time.sleep(10)
    
    # Iniciar el consumidor
    consume_transactions()