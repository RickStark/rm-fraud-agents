import re
import os
import warnings
import json
import time
import psycopg2
import unicodedata
from psycopg2.extras import RealDictCursor
from kafka import KafkaConsumer

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

# Evita el conflicto verificando si ya existe uno
if not isinstance(trace.get_tracer_provider(), TracerProvider):
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(
        OTLPSpanExporter(endpoint="http://phoenix:6006/v1/traces")
    ))
    trace.set_tracer_provider(provider)

from openinference.instrumentation.crewai import CrewAIInstrumentor
CrewAIInstrumentor().instrument()

# Suprimir warnings
warnings.filterwarnings("ignore", message=".*TracerProvider.*")
# 3. Recién importar crewai
from crewai import Agent, Task, Crew, Process, LLM

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",   "kafka:9092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC",    "transactions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fraud-detection-group")

DB_HOST     = os.getenv("DB_HOST",     "postgres")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "fraud_detection")
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

OLLAMA_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")

print(f"🔌 Conectando al LLM Local en: {OLLAMA_URL}")
print(f"📊 Conectando a PostgreSQL: {DB_HOST}:{DB_PORT}/{DB_NAME}")
print(f"📨 Conectando a Kafka: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}")

# ==========================================
# 2. CONFIGURACIÓN DEL LLM
# ==========================================
llm = LLM(
    model="ollama/qwen2.5:7b",
    base_url=OLLAMA_URL,
    temperature=0.2
)

# ==========================================
# 3. DEFINICIÓN DE AGENTES
# ==========================================
fraud_analyst = Agent(
    role='Senior Fraud Analyst',
    goal='Analizar transacciones financieras y evaluar el riesgo.',
    backstory=(
        "Eres un analista de fraude bancario. Sigue estas reglas de score EXACTAS:\n\n"

        "SCORES POR UBICACIÓN + RATIO (aplica el primero que coincida):\n"
        "1. País diferente SIN aviso + ratio >= 7x                → score 90 (BLOCK)\n"
        "2. País diferente SIN aviso + monto > $100               → score 70 (FLAG)\n"
        "3. País diferente CON aviso                              → score 50 (FLAG)\n"
        "4. Misma ubicación + ratio >= 7x                         → score 60 (FLAG)\n"
        "5. Misma ubicación + ratio 3x-7x                         → score 45 (FLAG)\n"
        "6. Misma ubicación + ratio < 3x + categoría nueva        → score 45 (FLAG)\n"
        "7. Misma ubicación + ratio < 3x + categoría conocida     → score 20 (APPROVE)\n\n"

        "COMERCIOS DE ALTO RIESGO (suman +20 al score):\n"
        "CryptoExchange, Wire Transfer, Gambling\n\n"

        "REGLAS INAMOVIBLES:\n"
        "- Misma ubicación incluye variaciones: 'Lima, Peru' = 'Lima, Perú'\n"
        "- Un monto menor al promedio NO es sospechoso\n"
        "- Responde SOLO con el análisis. Sin saludos ni despedidas.\n"
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
        "Eres el sistema automatizado de cumplimiento bancario. "
        "Aplica ÚNICAMENTE estas reglas:\n"
        "- Score >= 80 → BLOCK\n"
        "- Score 40-79 → FLAG\n"
        "- Score < 40  → APPROVE\n\n"
        "PROHIBIDO: saludos, despedidas, explicaciones extra.\n"
        "PROHIBIDO: inventar un score nuevo.\n"
        "Responde SOLO con:\n"
        "Decisión: [BLOCK/FLAG/APPROVE]\n"
        "Justificación: [1 línea]\n"
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
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT,
        database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    return conn

def get_transaction_details(tx_id):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT tx_id, user_id, amount, location, merchant, timestamp
                FROM transactions WHERE tx_id = %s
            """, (tx_id,))
            result = cursor.fetchone()
            return dict(result) if result else None
    finally:
        conn.close()

def get_user_historical_profile(user_id):
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT country_of_residence, has_travel_notice
                FROM user_profiles WHERE user_id = %s
            """, (user_id,))
            profile = cursor.fetchone()
            if not profile:
                return None

            cursor.execute("""
                SELECT AVG(amount) as avg_ticket,
                       ARRAY_AGG(DISTINCT merchant_category) as categories
                FROM transactions
                WHERE user_id = %s
                  AND timestamp >= NOW() - INTERVAL '30 days'
                  AND timestamp < (
                      SELECT timestamp FROM transactions
                      WHERE user_id = %s ORDER BY timestamp DESC LIMIT 1
                  )
            """, (user_id, user_id))
            stats = cursor.fetchone()

            return {
                "country_of_residence":    profile["country_of_residence"],
                "avg_ticket_last_30_days": float(stats["avg_ticket"]) if stats["avg_ticket"] else 0.0,
                "frequent_categories":     stats["categories"] if stats["categories"] else [],
                "has_travel_notice":       profile["has_travel_notice"]
            }
    finally:
        conn.close()

def save_fraud_analysis_result(tx_id, risk_score, decision, analysis_text):
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

def save_agent_metrics(tx_id, elapsed_time, total_tokens, decision):
    """Guarda métricas de latencia y tokens en BD"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO agent_metrics (tx_id, elapsed_seconds, total_tokens, decision)
                VALUES (%s, %s, %s, %s)
            """, (tx_id, elapsed_time, total_tokens, decision))
            conn.commit()
            print(f"📊 Métricas guardadas — {elapsed_time:.2f}s | tokens: {total_tokens}")
    finally:
        conn.close()

# ==========================================
# 5. GUARDRAIL
# ==========================================
def validate_decision(result_str):
    """Guardrail: asegura que la decisión sea válida"""
    valid = ["BLOCK", "FLAG", "APPROVE"]
    for d in valid:
        if d in result_str.upper():
            return d
    return "FLAG"  # fallback seguro

def normalize(text):
        return unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode().lower().strip()

# ==========================================
# 6. PROCESAMIENTO DE TRANSACCIONES
# ==========================================
def process_transaction(tx_data, user_history):
    print(f"\n⚡ Iniciando análisis local para TX: {tx_data['tx_id']}...")

    # Pre-calcular ratio para evitar errores del LLM
    amount     = float(tx_data.get('amount', 0))
    avg_ticket = float(user_history.get('avg_ticket_last_30_days', 1))
    ratio      = amount / avg_ticket if avg_ticket > 0 else 0

    # normaliza tildes antes de comparar
    same_location = normalize(tx_data.get('location','')) == normalize(user_history.get('country_of_residence',''))
    

    analysis_task = Task(
        description=(
            f"Analiza esta transacción aplicando las reglas de tu backstory.\n\n"
            f"DATOS:\n{json.dumps(tx_data, default=str, indent=2)}\n\n"
            f"HISTORIAL:\n{json.dumps(user_history, indent=2)}\n\n"
            f"DATOS PRE-CALCULADOS (usa estos, NO recalcules):\n"
            f"- Ubicaciones iguales: {same_location}\n"
            f"- Ratio de monto: {ratio:.2f}x\n"
            f"- Comercio de alto riesgo: {any(r in tx_data.get('merchant','') for r in ['Crypto','Wire','Gambling'])}\n"
        ),
        expected_output=(
            "1. Ubicaciones: [iguales/diferentes]\n"
            "2. Ratio: [valor]x\n"
            "3. Comercio: [categoría] - [habitual/nuevo]\n"
            "4. Justificación del score\n"
            "5. Score: [0-100]"
        ),
        agent=fraud_analyst
    )

    decision_task = Task(
        description=(
            "Extrae el score del análisis previo y aplica las reglas de tu backstory.\n"
            "NO inventes score. NO agregues saludos ni despedidas."
        ),
        expected_output=(
            "Decisión: [BLOCK/FLAG/APPROVE]\n"
            "Justificación: [1 línea]"
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
    result      = fraud_crew.kickoff()
    elapsed     = time.time() - start_time

    print(f"\n✅ Tiempo de ejecución: {elapsed:.2f}s")
    print(f"Resultado:\n{result}")

    # Extraer decisión con guardrail
    result_str = str(result)
    decision   = validate_decision(result_str)
    # Extraer score real del LLM
    match = re.search(r'[Ss]core[:\s]+(\d+)', result_str)
    risk_score = int(match.group(1)) if match else (
        85 if decision == "BLOCK" else (50 if decision == "FLAG" else 30)
    )

    # Guardar métricas
    try:
        total_tokens = fraud_crew.usage_metrics.total_tokens
    except Exception:
        total_tokens = 0
    save_agent_metrics(tx_data['tx_id'], elapsed, total_tokens, decision)

    # Guardar resultado
    save_fraud_analysis_result(tx_data['tx_id'], risk_score, decision, result_str)

    return result

# ==========================================
# 7. CONSUMIDOR DE KAFKA
# ==========================================
def consume_transactions():
    print(f"\n🎧 Iniciando consumidor de Kafka...")
    print(f"   Broker: {KAFKA_BROKER} | Topic: {KAFKA_TOPIC} | Group: {KAFKA_GROUP_ID}\n")

    max_retries = 20
    retry_count = 0

    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                session_timeout_ms=60000,
                heartbeat_interval_ms=20000,
                max_poll_interval_ms=600000,   # 10 min — el LLM puede tardar
                request_timeout_ms=120000,     # debe ser < connections_max_idle_ms (540000)
                max_poll_records=1,
            )
            consumer.poll(timeout_ms=3000)
            print("✅ Conectado a Kafka exitosamente!")
            break
        except Exception as e:
            retry_count += 1
            print(f"⏳ Esperando a Kafka... Intento {retry_count}/{max_retries}: {type(e).__name__}: {e}")
            time.sleep(10)

    if retry_count >= max_retries:
        print("❌ No se pudo conectar a Kafka después de varios intentos")
        return

    print("👀 Esperando mensajes...\n")
    AMOUNT_THRESHOLD = 300

    try:
        for message in consumer:
            try:
                kafka_data = message.value
                tx_id      = kafka_data.get('tx_id')
                print(f"\n📨 Mensaje recibido: {kafka_data}")

                tx_data = get_transaction_details(tx_id)
                if not tx_data:
                    print(f"⚠️  TX {tx_id} no encontrada en BD")
                    continue

                user_history = get_user_historical_profile(tx_data['user_id'])
                if not user_history:
                    print(f"⚠️  Perfil de usuario {tx_data['user_id']} no encontrado")
                    continue

                amount = float(tx_data['amount'])
                if amount > AMOUNT_THRESHOLD:
                    process_transaction(tx_data, user_history)
                    print(f"✅ TX {tx_id} procesada (monto: {amount})")
                else:
                    print(f"ℹ️  TX {tx_id} omitida — monto bajo: {amount}")

            except Exception as e:
                print(f"❌ Error procesando mensaje: {e}")
                continue

    except KeyboardInterrupt:
        print("\n👋 Deteniendo consumidor...")
    finally:
        consumer.close()

# ==========================================
# 8. PUNTO DE ENTRADA
# ==========================================
if __name__ == "__main__":
    print("⏳ Esperando a que los servicios estén listos...")
    time.sleep(30)
    consume_transactions()