#!/usr/bin/env python3
"""
Script de utilidad para enviar transacciones manuales al sistema
Útil para pruebas y demos
"""

import json
import sys
import psycopg2
from kafka import KafkaProducer
from datetime import datetime

# Configuración
KAFKA_BROKER = "localhost:9093"
KAFKA_TOPIC = "transactions"
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "fraud_detection",
    "user": "postgres",
    "password": "postgres"
}

def insert_transaction_to_db(tx_data):
    """Inserta la transacción en PostgreSQL"""
    conn = psycopg2.connect(**DB_CONFIG)
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
            print(f"✅ Transacción insertada en BD: {tx_data['tx_id']}")
    finally:
        conn.close()

def send_to_kafka(tx_id):
    """Envía el tx_id a Kafka"""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    kafka_message = {
        "tx_id": tx_id,
        "timestamp": datetime.now().isoformat()
    }
    
    producer.send(KAFKA_TOPIC, value=kafka_message)
    producer.flush()
    producer.close()
    print(f"📨 Mensaje enviado a Kafka: {tx_id}")

def main():
    print("\n🚀 Enviar Transacción Manual al Sistema de Detección de Fraude\n")
    
    # Pedir datos de la transacción
    tx_id = input("TX ID (ej: TX_TEST_001): ").strip()
    user_id = input("User ID (ej: USR_105): ").strip()
    amount = float(input("Monto (ej: 2500.00): ").strip())
    location = input("Ubicación (ej: Moscú, Rusia): ").strip()
    merchant = input("Comercio (ej: CryptoExchange Ltd.): ").strip()
    category = input("Categoría (ej: Crypto): ").strip()
    
    # Crear objeto de transacción
    tx_data = {
        "tx_id": tx_id,
        "user_id": user_id,
        "amount": amount,
        "location": location,
        "merchant": merchant,
        "category": category,
        "timestamp": datetime.now().isoformat()
    }
    
    print("\n📝 Resumen de la transacción:")
    print(json.dumps(tx_data, indent=2))
    
    confirm = input("\n¿Enviar esta transacción? (s/n): ").strip().lower()
    
    if confirm == 's':
        try:
            # Insertar en BD
            insert_transaction_to_db(tx_data)
            
            # Enviar a Kafka
            send_to_kafka(tx_id)
            
            print("\n✅ ¡Transacción enviada exitosamente!")
            print("Revisa los logs de fraud-detection-app para ver el análisis:")
            print("docker logs -f fraud-detection-app")
            
        except Exception as e:
            print(f"\n❌ Error: {e}")
            sys.exit(1)
    else:
        print("\n❌ Operación cancelada")

if __name__ == "__main__":
    main()
