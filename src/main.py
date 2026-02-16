import os
import json
import time
from crewai import Agent, Task, Crew, Process,LLM
#from langchain_community.chat_models import ChatOllama

# ==========================================
# 1. CONFIGURACIÓN DEL LLM LOCAL (Ollama)
# ==========================================
# Conecta con el contenedor "ollama" a través de la red de Docker
ollama_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

print(f"🔌 Conectando al LLM Local en: {ollama_url}")

# Usaremos 'mistral' o 'llama3' (modelos muy capaces y ligeros para correr en local)
llm = LLM(
    model="ollama/mistral",
    base_url=ollama_url,
    temperature=0.1
)

# ==========================================
# 2. DEFINICIÓN DE AGENTES
# ==========================================
fraud_analyst = Agent(
    role='Senior Fraud Analyst',
    goal='Analizar transacciones financieras y evaluar el riesgo.',
    backstory=(
        "Eres un analista implacable y estricto de prevención de fraude bancario. "
        "Tu idioma nativo es el español. "
        "Consideras que cualquier transacción internacional sin aviso de viaje por un monto mayor "
        "a 7 veces el ticket promedio es ALTAMENTE SOSPECHOSA (Riesgo > 80). "
        "Eres deductivo, directo y justificas tus decisiones en español."
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
# 3. EJECUCIÓN DEL FLUJO
# ==========================================
def process_transaction(tx_data, user_history):
    print(f"\n⚡ Iniciando análisis local para TX: {tx_data['tx_id']}...")
    
    analysis_task = Task(
        description=(
            f"Transacción actual: {json.dumps(tx_data)}. "
            f"Historial del usuario: {json.dumps(user_history)}. "
            "Paso 1: Analiza la diferencia de montos, ubicaciones y el comercio. "
            "Paso 2: Calcula un score de riesgo del 0 al 100 siendo muy estricto. "
        ),
        expected_output="Un reporte  detallando los hallazgos y un score numérico de riesgo al final.",
        agent=fraud_analyst
    )

    decision_task = Task(
        description=(
            "Lee el análisis de riesgo previo. "
            "Reglas: Riesgo > 80 = 'BLOCK'. Riesgo entre 40 y 80 = 'FLAG'. Riesgo < 40 = 'APPROVE'. "
        ),
        expected_output="Palabra clave (BLOCK, FLAG o APPROVE) seguida de un breve resumen de 2 líneas.",
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
    print(f"\n✅ Tiempo de ejecución local: {time.time() - start_time:.2f}s")
    print(f"Resultado:\n{result}")

if __name__ == "__main__":
    # Simulamos el evento que llega por Kafka / RabbitMQ
    incoming_tx = {
        "tx_id": "TX_9982",
        "user_id": "USR_105",
        "amount": 2500.00,
        "location": "Moscú, Rusia",
        "merchant": "CryptoExchange Ltd."
    }
    
    # En un sistema real, esto se obtendría conectando una Herramienta (Tool) a una Base de Datos
    historical_profile = {
        "country_of_residence": "Lima, Peru",
        "avg_ticket_last_30_days": 150.00,
        "frequent_categories": ["Groceries", "Restaurants", "Streaming"],
        "has_travel_notice": False
    }
    process_transaction(incoming_tx, historical_profile)