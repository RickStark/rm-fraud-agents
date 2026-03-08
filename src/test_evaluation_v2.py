import re
from main import process_transaction, validate_decision
from unittest.mock import patch

TEST_CASES = [
    # ===== ALTO RIESGO - BLOCK =====
    {
        "description": "País diferente SIN aviso + monto >7x",
        "tx":      {"tx_id": "TEST_001", "amount": 5000, "location": "Tokyo, Japan", "merchant": "CryptoExchange"},
        "history": {"country_of_residence": "Lima, Peru", "avg_ticket_last_30_days": 200, "frequent_categories": ["Groceries"], "has_travel_notice": False},
        "expected": "BLOCK"
    },
    {
        "description": "Comercio de alto riesgo + país diferente",
        "tx":      {"tx_id": "TEST_002", "amount": 3000, "location": "Moscow, Russia", "merchant": "Wire Transfer Service"},
        "history": {"country_of_residence": "Lima, Peru", "avg_ticket_last_30_days": 300, "frequent_categories": ["Groceries"], "has_travel_notice": False},
        "expected": "BLOCK"
    },

    # ===== MEDIO RIESGO - FLAG =====
    {
        "description": "País diferente CON aviso de viaje",
        "tx":      {"tx_id": "TEST_003", "amount": 800, "location": "Madrid, Spain", "merchant": "Zara"},
        "history": {"country_of_residence": "Lima, Peru", "avg_ticket_last_30_days": 300, "frequent_categories": ["Shopping"], "has_travel_notice": True},
        "expected": "FLAG"
    },
    {
        "description": "Monto 4x promedio en ubicación usual",
        "tx":      {"tx_id": "TEST_004", "amount": 1200, "location": "Lima, Peru", "merchant": "Samsung Store"},
        "history": {"country_of_residence": "Lima, Peru", "avg_ticket_last_30_days": 300, "frequent_categories": ["Groceries"], "has_travel_notice": False},
        "expected": "FLAG"
    },

    # ===== BAJO RIESGO - APPROVE =====
    {
        "description": "Misma ciudad + monto normal + categoría conocida",
        "tx":      {"tx_id": "TEST_005", "amount": 150, "location": "Lima, Peru", "merchant": "Wong Supermercado"},
        "history": {"country_of_residence": "Lima, Peru", "avg_ticket_last_30_days": 200, "frequent_categories": ["Groceries"], "has_travel_notice": False},
        "expected": "APPROVE"
    },
    {
        "description": "Monto menor al promedio",
        "tx":      {"tx_id": "TEST_006", "amount": 50, "location": "Lima, Peru", "merchant": "Starbucks"},
        "history": {"country_of_residence": "Lima, Peru", "avg_ticket_last_30_days": 200, "frequent_categories": ["Restaurants"], "has_travel_notice": False},
        "expected": "APPROVE"
    },

    # ===== EDGE CASES =====
    {
        "description": "Variación de escritura misma ciudad (Lima, Peru vs Lima, Perú)",
        "tx":      {"tx_id": "TEST_007", "amount": 200, "location": "Lima, Perú", "merchant": "Plaza Vea"},
        "history": {"country_of_residence": "Lima, Peru", "avg_ticket_last_30_days": 200, "frequent_categories": ["Groceries"], "has_travel_notice": False},
        "expected": "APPROVE"
    },##se equivoco
    {
        "description": "Monto exacto en límite 7x",
        "tx":      {"tx_id": "TEST_008", "amount": 1400, "location": "Lima, Peru", "merchant": "LG Store"},
        "history": {"country_of_residence": "Lima, Peru", "avg_ticket_last_30_days": 200, "frequent_categories": ["Electronics"], "has_travel_notice": False},
        "expected": "FLAG"
    },
]

def run_evaluation():
    results = []
    for case in TEST_CASES:
        print(f"\n🧪 {case['description']}")
        with patch("main.save_fraud_analysis_result"), \
             patch("main.save_agent_metrics"):
            result   = process_transaction(case["tx"], case["history"])
            decision = validate_decision(str(result))
            passed   = decision == case["expected"]
            results.append(passed)
            print(f"{'✅' if passed else '❌'} Esperado: {case['expected']} | Obtenido: {decision}")

    accuracy = sum(results) / len(results) * 100
    print(f"\n📊 Accuracy: {accuracy:.0f}% ({sum(results)}/{len(results)})")
    
    # Falla el build si accuracy < 80%
    assert accuracy >= 80, f"❌ Accuracy insuficiente: {accuracy:.0f}%"

if __name__ == "__main__":
    run_evaluation()




