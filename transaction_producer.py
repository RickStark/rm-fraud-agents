import os
import json
import time
import random
import psycopg2
from kafka import KafkaProducer
from datetime import datetime

# ==========================================
# CONFIGURACIÓN
# ==========================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "fraud_detection")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

# ==========================================
# DATOS DE EJEMPLO
# ==========================================
USUARIOS = ["USR_105", "USR_106"]

TRANSACCIONES_NORMALES = [
    {"location": "Lima, Peru", "merchant": "Supermercados Wong", "category": "Groceries", "amount_range": (50, 200)},
    {"location": "Lima, Peru", "merchant": "Restaurant Central", "category": "Restaurants", "amount_range": (30, 150)},
    {"location": "Lima, Peru", "merchant": "Netflix", "category": "Streaming", "amount_range": (10, 20)},
    {"location": "Lima, Peru", "merchant": "Tottus", "category": "Groceries", "amount_range": (80, 250)},
    {"location": "Buenos Aires, Argentina", "merchant": "Carrefour", "category": "Groceries", "amount_range": (100, 300)},
]

TRANSACCIONES_SOSPECHOSAS = [
    {"location": "Moscú, Rusia", "merchant": "CryptoExchange Ltd.", "category": "Crypto", "amount_range": (2000, 5000)},
    {"location": "Lagos, Nigeria", "merchant": "Wire Transfer Ltd", "category": "Financial", "amount_range": (3000, 8000)},
    {"location": "Shenzhen, China", "merchant": "Electronics Wholesale", "category": "Electronics", "amount_range": (2500, 6000)},
    {"location": "Dubai, UAE", "merchant": "Luxury Cars International", "category": "Automotive", "amount_range": (5000, 15000)},
]

# ==========================================
# FUNCIONES
# ==========================================
def get_db_connection():
    """Establece conexión con PostgreSQL"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def insert_transaction_to_db(tx_data):
    """Inserta la transacción en la base de datos"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO transactions (tx_id, user_id, amount, location, merchant, merchant_category, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tx_id) DO NOTHING
            """, (
                tx_data['tx_id'],
                tx_data['user_id'],
                tx_data['amount'],
                tx_data['location'],
                tx_data['merchant'],
                tx_data['category'],
                tx_data['timestamp']
            ))
            conn.commit()
            print(f"✅ Transacción {tx_data['tx_id']} insertada en BD")
    finally:
        conn.close()

def generate_transaction(tx_id_counter, is_suspicious=False):
    """Genera una transacción aleatoria"""
    user_id = random.choice(USUARIOS)
    
    if is_suspicious:
        template = random.choice(TRANSACCIONES_SOSPECHOSAS)
    else:
        template = random.choice(TRANSACCIONES_NORMALES)
    
    amount = round(random.uniform(template['amount_range'][0], template['amount_range'][1]), 2)
    
    tx_data = {
        "tx_id": f"TX_{tx_id_counter:06d}",
        "user_id": user_id,
        "amount": amount,
        "location": template['location'],
        "merchant": template['merchant'],
        "category": template['category'],
        "timestamp": datetime.now().isoformat()
    }
    
    return tx_data

def send_to_kafka(producer, tx_data):
    """Envía la transacción a Kafka"""
    # Solo enviamos el tx_id a Kafka, el resto se consulta desde la BD
    kafka_message = {
        "tx_id": tx_data['tx_id'],
        "timestamp": tx_data['timestamp']
    }
    
    producer.send(
        KAFKA_TOPIC,
        value=kafka_message
    )
    producer.flush()
    print(f"📨 Mensaje enviado a Kafka: {kafka_message}")

def main():
    print(f"🚀 Iniciando Transaction Producer...")
    print(f"   Kafka Broker: {KAFKA_BROKER}")
    print(f"   Topic: {KAFKA_TOPIC}")
    print(f"   PostgreSQL: {DB_HOST}:{DB_PORT}/{DB_NAME}\n")
    
    # Esperar a que Kafka esté disponible
    time.sleep(15)
    
    # Configurar el producer de Kafka
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print("✅ Producer conectado a Kafka\n")
    print("🎲 Generando transacciones cada 30 segundos...")
    print("   - 70% normales")
    print("   - 30% sospechosas\n")
    
    tx_id_counter = 10000
    
    try:
        while True:
            # Decidir si es sospechosa (30% de probabilidad)
            is_suspicious = random.random() < 0.3
            
            # Generar transacción
            tx_data = generate_transaction(tx_id_counter, is_suspicious)
            tx_id_counter += 1
            
            # Guardar en BD
            insert_transaction_to_db(tx_data)
            
            # Enviar a Kafka
            send_to_kafka(producer, tx_data)
            
            status = "🚨 SOSPECHOSA" if is_suspicious else "✅ NORMAL"
            print(f"{status} | TX: {tx_data['tx_id']} | Usuario: {tx_data['user_id']} | ${tx_data['amount']:.2f} | {tx_data['location']}\n")
            
            # Esperar 30 segundos
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\n👋 Deteniendo producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
