import os
import json
import time
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaConsumer
from crewai import Agent, Task, Crew, Process, LLM

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

# vLLM (via OpenAI compatible API)
VLLM_BASE_URL = os.getenv("VLLM_BASE_URL", "http://vllm:8000/v1")
VLLM_MODEL = os.getenv("VLLM_MODEL", "TinyLlama/TinyLlama-1.1B-Chat-v1.0")

print(f"🔌 Conectando a vLLM en: {VLLM_BASE_URL}")
print(f"📊 Conectando a PostgreSQL: {DB_HOST}:{DB_PORT}/{DB_NAME}")
print(f"📨 Conectando a Kafka: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}")

# ==========================================
# 2. CONFIGURACIÓN DEL LLM (vLLM via CrewAI)
# ==========================================
# CrewAI puede usar vLLM a través de la API compatible con OpenAI
llm = LLM(
    model=f"openai/{VLLM_MODEL}",  # Prefijo openai/ para usar API OpenAI
    base_url=VLLM_BASE_URL,
    api_key="dummy-key",  # vLLM no requiere key real
    temperature=0.0
)

# ==========================================
# 3. DEFINICIÓN DE AGENTES MULTI-AGENTE
# ==========================================
fraud_analyst = Agent(
    role='Senior Fraud Analyst',
    goal='Analizar transacciones financieras y evaluar el riesgo de fraude de manera precisa.',
    backstory=(
        "Eres un analista experto de prevención de fraude bancario. Tu idioma nativo es el español. "
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
        "Eres preciso con los cálculos matemáticos. NO inventes números."
    ),
    verbose=True,
    allow_delegation=False,
    llm=llm,
    tools=[]
)

risk_calculator = Agent(
    role='Risk Score Calculator',
    goal='Calcular el score numérico de riesgo basado en el análisis del fraud analyst.',
    backstory=(
        "Eres un sistema cuantitativo que traduce análisis cualitativos a scores numéricos. "
        "Tu trabajo es SOLO asignar un número del 0 al 100 basándote en el análisis previo. "
        "Eres muy estricto con las reglas matemáticas y NUNCA te equivocas en cálculos."
    ),
    verbose=True,
    allow_delegation=False,
    llm=llm,
    tools=[]
)

decision_maker = Agent(
    role='Risk Decision Executor',
    goal='Tomar la decisión final (BLOCK/FLAG/APPROVE) basándose en el score de riesgo.',
    backstory=(
        "Eres el sistema automatizado de cumplimiento que toma decisiones irrevocables. "
        "Aplicas las reglas de negocio de forma estricta sin excepciones. "
        "Tu única fuente de verdad es el score numérico del risk calculator."
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
# 5. PROCESAMIENTO MULTI-AGENTE
# ==========================================
def process_transaction(tx_data, user_history):
    """Procesa una transacción con los agentes de CrewAI usando vLLM"""
    print(f"\n⚡ Iniciando análisis MULTI-AGENTE para TX: {tx_data['tx_id']}...")

    # -------------------------------------------------------
    # CÁLCULOS EN PYTHON (para ayudar a los agentes)
    # -------------------------------------------------------
    amount = float(tx_data.get('amount', 0))
    avg_ticket = float(user_history.get('avg_ticket_last_30_days', 1))
    ratio = round(amount / avg_ticket, 2) if avg_ticket > 0 else 0

    tx_location = str(tx_data.get('location', '')).strip().lower()
    residence = str(user_history.get('country_of_residence', '')).strip().lower()
    same_location = any(word in tx_location for word in residence.split(','))

    has_travel = user_history.get('has_travel_notice', False)

    print(f"   📊 Datos pre-calculados → ratio: {ratio}x | misma ubicación: {same_location}")

    # -------------------------------------------------------
    # TAREA 1: FRAUD ANALYST - Análisis cualitativo
    # -------------------------------------------------------
    analysis_task = Task(
        description=(
            f"🔍 ANALIZA ESTA TRANSACCIÓN (PASO 1 de 3)\n\n"
            f"━━━ DATOS DE LA TRANSACCIÓN ━━━\n"
            f"TX ID       : {tx_data.get('tx_id')}\n"
            f"Usuario     : {tx_data.get('user_id')}\n"
            f"Monto       : ${amount}\n"
            f"Ubicación   : {tx_data.get('location')}\n"
            f"Comercio    : {tx_data.get('merchant')}\n\n"
            f"━━━ HISTORIAL DEL USUARIO ━━━\n"
            f"Residencia  : {user_history.get('country_of_residence')}\n"
            f"Promedio 30d: ${avg_ticket}\n"
            f"Categorías  : {user_history.get('frequent_categories')}\n"
            f"Aviso viaje : {has_travel}\n\n"
            f"━━━ AYUDA MATEMÁTICA ━━━\n"
            f"Ratio calculado: {ratio}x el promedio\n"
            f"Ubicaciones: {'MISMAS' if same_location else 'DIFERENTES'}\n\n"
            f"TU TAREA:\n"
            f"1. Evalúa si las ubicaciones son iguales o diferentes\n"
            f"2. Verifica el ratio: ¿Es {ratio}x mayor o menor a 7x?\n"
            f"3. Analiza si el comercio es usual para el usuario\n"
            f"4. Considera el aviso de viaje\n"
            f"5. Escribe un análisis cualitativo en español\n\n"
            f"NO CALCULES EL SCORE TODAVÍA. Solo analiza."
        ),
        expected_output=(
            "Análisis cualitativo que incluya:\n"
            "- Comparación de ubicaciones (misma/diferente)\n"
            "- Interpretación del ratio de monto\n"
            "- Evaluación del tipo de comercio\n"
            "- Factores de riesgo identificados\n"
            "- Factores de seguridad identificados"
        ),
        agent=fraud_analyst
    )

    # -------------------------------------------------------
    # TAREA 2: RISK CALCULATOR - Asignar score numérico
    # -------------------------------------------------------
    scoring_task = Task(
        description=(
            f"🔢 CALCULA EL SCORE DE RIESGO (PASO 2 de 3)\n\n"
            f"Lee el análisis anterior del Fraud Analyst.\n\n"
            f"DATOS CONFIRMADOS:\n"
            f"- Ratio: {ratio}x\n"
            f"- Ubicaciones: {'MISMAS' if same_location else 'DIFERENTES'}\n"
            f"- Aviso de viaje: {'SÍ' if has_travel else 'NO'}\n\n"
            f"REGLAS PARA ASIGNAR SCORE:\n"
            f"1. Ubicación DIFERENTE + SIN aviso → 80-100\n"
            f"2. Ubicación DIFERENTE + CON aviso → 45-65\n"
            f"3. Ubicación MISMA + ratio >= 7x → 65-79\n"
            f"4. Ubicación MISMA + ratio 3-7x → 40-60\n"
            f"5. Ubicación MISMA + ratio < 3x → 10-35\n\n"
            f"TU TAREA:\n"
            f"Asigna un score del 0 al 100 siguiendo EXACTAMENTE estas reglas.\n"
            f"Responde SOLO con el número."
        ),
        expected_output=(
            "Un número del 0 al 100 (solo el número, sin texto adicional)"
        ),
        agent=risk_calculator
    )

    # -------------------------------------------------------
    # TAREA 3: DECISION MAKER - Decisión final
    # -------------------------------------------------------
    decision_task = Task(
        description=(
            f"⚖️ TOMA LA DECISIÓN FINAL (PASO 3 de 3)\n\n"
            f"Lee el score que calculó el Risk Calculator.\n\n"
            f"REGLAS DE DECISIÓN (INAMOVIBLES):\n"
            f"- Score >= 80 → BLOCK\n"
            f"- Score 40-79 → FLAG\n"
            f"- Score < 40 → APPROVE\n\n"
            f"TU TAREA:\n"
            f"1. Extrae el score del mensaje anterior\n"
            f"2. Aplica la regla correspondiente\n"
            f"3. Responde en formato:\n"
            f"   Decisión: [BLOCK/FLAG/APPROVE]\n"
            f"   Score: [número]\n"
            f"   Razón: [1 línea]"
        ),
        expected_output=(
            "Decisión: [BLOCK/FLAG/APPROVE]\n"
            "Score: [número]\n"
            "Razón: [justificación breve]"
        ),
        agent=decision_maker
    )

    # -------------------------------------------------------
    # CREW: Ejecutar los 3 agentes secuencialmente
    # -------------------------------------------------------
    fraud_crew = Crew(
        agents=[fraud_analyst, risk_calculator, decision_maker],
        tasks=[analysis_task, scoring_task, decision_task],
        process=Process.sequential,  # Uno tras otro
        verbose=True
    )

    print(f"\n🤖 Ejecutando crew de 3 agentes con vLLM...")
    start_time = time.time()
    result = fraud_crew.kickoff()
    elapsed_time = time.time() - start_time

    print(f"\n✅ Análisis multi-agente completado en {elapsed_time:.2f}s")
    print(f"📋 Resultado final:\n{result}")

    # -------------------------------------------------------
    # Extraer decisión del resultado
    # -------------------------------------------------------
    result_str = str(result).upper()
    if "BLOCK" in result_str:
        decision = "BLOCK"
        risk_score = 85
    elif "APPROVE" in result_str:
        decision = "APPROVE"
        risk_score = 25
    else:
        decision = "FLAG"
        risk_score = 55

    # Override de seguridad (Python como última barrera)
    if not same_location and not has_travel and decision == "APPROVE":
        print(f"⚠️  OVERRIDE: Ubicación diferente sin aviso → BLOCK")
        decision = "BLOCK"
        risk_score = 85

    # Guardar resultado en BD
    save_fraud_analysis_result(tx_data['tx_id'], risk_score, decision, str(result))
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

    max_retries = 10
    retry_count = 0

    while retry_count < max_retries:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset='earliest',
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                # Timeouts para vLLM (más rápido que Ollama pero aún necesita tiempo)
                session_timeout_ms=300000,      # 5 minutos
                heartbeat_interval_ms=30000,    # 30 segundos
                max_poll_interval_ms=300000,    # 5 minutos
                request_timeout_ms=330000,
                enable_auto_commit=False,
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
        while True:
            messages = consumer.poll(timeout_ms=5000, max_records=1)

            if not messages:
                continue

            for partition, msgs in messages.items():
                for message in msgs:
                    try:
                        kafka_data = message.value
                        tx_id = kafka_data.get('tx_id')

                        print(f"\n📨 Mensaje recibido de Kafka: {kafka_data}")

                        # Obtener datos completos desde BD
                        tx_data = get_transaction_details(tx_id)
                        if not tx_data:
                            print(f"⚠️  Transacción {tx_id} no encontrada en la base de datos")
                            consumer.commit()
                            continue

                        # Obtener perfil histórico del usuario
                        user_history = get_user_historical_profile(tx_data['user_id'])
                        if not user_history:
                            print(f"⚠️  Perfil de usuario {tx_data['user_id']} no encontrado")
                            consumer.commit()
                            continue

                        # Procesar con multi-agente
                        # Procesar según el monto
                        amount = float(tx_data['amount'])
                        if amount > AMOUNT_THRESHOLD:
                            process_transaction(tx_data, user_history)
                            print(f"✅ Transacción {tx_id} procesada (monto: {amount})")
                        else:
                            print(f"ℹ️  Transacción {tx_id} omitida - monto bajo: {amount}")

                        # Commit solo después de procesar exitosamente
                        consumer.commit()
                        print(f"✅ Offset commiteado para TX: {tx_id}")

                    except Exception as e:
                        print(f"❌ Error procesando mensaje: {e}")
                        import traceback
                        traceback.print_exc()
                        consumer.commit()
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
    time.sleep(120)

    # Iniciar el consumidor
    consume_transactions()
