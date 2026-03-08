import re
from main import process_transaction, validate_decision
from unittest.mock import patch

# Test cases con resultado esperado
TEST_CASES = [
    {
        "tx":      {"tx_id": "TEST_001", "amount": 5000, "location": "Tokyo, Japan", "merchant": "CryptoExchange"},
        "history": {"country_of_residence": "Lima, Peru", "avg_ticket_last_30_days": 200, "frequent_categories": ["Groceries"], "has_travel_notice": False},
        "expected": "BLOCK"
    },
    {
        "tx":      {"tx_id": "TEST_002", "amount": 150, "location": "Lima, Peru", "merchant": "Wong Supermercado"},
        "history": {"country_of_residence": "Lima, Peru", "avg_ticket_last_30_days": 200, "frequent_categories": ["Groceries"], "has_travel_notice": False},
        "expected": "APPROVE"
    },
    {
        "tx":      {"tx_id": "TEST_003", "amount": 800, "location": "Madrid, Spain", "merchant": "Zara"},
        "history": {"country_of_residence": "Lima, Peru", "avg_ticket_last_30_days": 300, "frequent_categories": ["Shopping"], "has_travel_notice": True},
        "expected": "FLAG"
    },
]

def run_evaluation():
    results = []
    for case in TEST_CASES:
        with patch("main.save_fraud_analysis_result"), \
            patch("main.save_agent_metrics"):
            result = process_transaction(case["tx"], case["history"])
        #result    = process_transaction(case["tx"], case["history"])
        decision  = validate_decision(str(result))
        passed    = decision == case["expected"]
        results.append(passed)
        print(f"{'✅' if passed else '❌'} {case['tx']['tx_id']} — Esperado: {case['expected']} | Obtenido: {decision}")

    accuracy = sum(results) / len(results) * 100
    print(f"\n📊 Accuracy: {accuracy:.0f}% ({sum(results)}/{len(results)})")

if __name__ == "__main__":
    run_evaluation()




