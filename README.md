# 🛡️ Fraud Detection System — Multi-Agent with CrewAI + Ollama

Real-time fraud detection system using local AI agents, Kafka, and PostgreSQL.

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        fraud-network                        │
│                                                             │
│  [transaction-producer] ──► [Kafka] ──► [fraud-agents]     │
│                                              │              │
│                          [PostgreSQL] ◄──────┤              │
│                          [Phoenix]    ◄──────┘              │
│                          [Ollama]     ◄── qwen2.5:7b        │
└─────────────────────────────────────────────────────────────┘
```

### Services

| Service | Image | Port | Role |
|---|---|---|---|
| zookeeper | confluentinc/cp-zookeeper:7.5.0 | 2181 | Kafka coordination |
| kafka | confluentinc/cp-kafka:7.5.0 | 9092 | Message broker |
| kafka-ui | provectuslabs/kafka-ui | 8080 | Kafka monitoring UI |
| postgres | postgres:15-alpine | 5432 | Results storage |
| adminer | adminer | 8081 | Database UI |
| ollama | ollama/ollama | 11434 | Local LLM (GPU) |
| ollama-pull | ollama/ollama | — | Automatic model download |
| fraud-agents | custom | — | CrewAI agents |
| transaction-producer | custom | — | Transaction generator |
| phoenix | arizephoenix/phoenix | 6006 | Observability / Traces |

---

## 🤖 CrewAI Agents

### Agent 1 — Senior Fraud Analyst
- **Role:** Analyzes the transaction and calculates a risk score (0-100)
- **LLM:** `qwen2.5:7b` via Ollama (local, GPU)
- **Inputs:** transaction data + user historical profile
- **Output:** justified risk score report

### Agent 2 — Risk Decision Executor
- **Role:** Converts the score into a final decision
- **Rules:**
  - Score >= 80 → `BLOCK`
  - Score 40-79 → `FLAG`
  - Score < 40  → `APPROVE`

### Processing Flow

```
Kafka message
     │
     ▼
get_transaction_details()  ──► PostgreSQL
     │
     ▼
get_user_historical_profile()  ──► PostgreSQL
     │
     ▼
amount > $300?
     │ YES
     ▼
CrewAI Crew (sequential)
  ├── fraud_analyst  ──► Ollama (qwen2.5:7b)
  └── decision_maker ──► Ollama (qwen2.5:7b)
     │
     ▼
validate_decision() [guardrail]
     │
     ▼
save_fraud_analysis_result()  ──► PostgreSQL
save_agent_metrics()          ──► PostgreSQL
```

---

## 🧠 Technical Decisions

### Why qwen2.5:7b instead of mistral:7b?
`qwen2.5:7b` follows structured instructions with greater precision and makes fewer arithmetic errors than `mistral:7b`. Critical for a system that requires consistent numerical scores.

### Why are calculations pre-computed in Python?
Small LLMs (7B) fail at basic arithmetic. The ratio `amount / average` is calculated in Python and passed as a fixed value to the prompt, eliminating model calculation errors.

### Why numbered priority rules in the prompt?
LLMs follow ordered lists better than narrative paragraphs. Scoring rules were defined as a numbered list with explicit priority to reduce ambiguity.

### Why separate backstory from task description?
- **Backstory** = business rules (what to do)
- **Task description** = transaction data (what to work with)
Mixing both confuses the model and generates inconsistencies.

### Why a Guardrail in validate_decision()?
LLMs are probabilistic — they can return unexpected text. The guardrail ensures the final decision is always `BLOCK`, `FLAG`, or `APPROVE`, never an invalid value.

### Why extended Kafka timeouts?
`max_poll_interval_ms=600000` (10 min) because the local LLM on a GTX 1650 GPU can take several minutes per transaction. Without this adjustment, Kafka removes the consumer from the group.

---

## 📊 Evaluation Results

### Test Suite — 8 test cases

| # | Description | Expected | Result |
|---|---|---|---|
| TEST_001 | Different country NO travel notice + ratio 25x | BLOCK | ✅ BLOCK |
| TEST_002 | High-risk merchant (Wire Transfer) + different country | BLOCK | ✅ BLOCK |
| TEST_003 | Different country WITH travel notice | FLAG | ✅ FLAG |
| TEST_004 | Amount 4x average at usual location | FLAG | ✅ FLAG |
| TEST_005 | Same city + normal amount + known category | APPROVE | ✅ APPROVE |
| TEST_006 | Amount below average | APPROVE | ✅ APPROVE |
| TEST_007 | Spelling variation (Lima, Perú vs Lima, Peru) | APPROVE | ✅ APPROVE |
| TEST_008 | Exact 7x ratio at usual location | FLAG | ✅ FLAG |

**Final Accuracy: 100% (8/8)** ✅

### Performance Metrics

| Metric | Value |
|---|---|
| Average time per transaction | ~6s |
| GPU | NVIDIA RTX 5060 |
| Model | qwen2.5:7b (4.7GB) |
| Minimum amount threshold | $300 |

---

## 🔍 Observability — Arize Phoenix

Phoenix captures traces from each crew execution at `http://localhost:6006`.

**Available metrics:**
- Latency per agent (P50 / P99)
- Total traces executed
- Spans per task (fraud_analyst / decision_maker)

**Metrics in PostgreSQL:**
```sql
-- Decisions by type
SELECT decision, COUNT(*), AVG(elapsed_seconds)
FROM agent_metrics
GROUP BY decision;

-- Blocked transactions today
SELECT tx_id, risk_score, analyzed_at
FROM fraud_analysis_results
WHERE decision = 'BLOCK'
  AND analyzed_at >= CURRENT_DATE;
```

---

## 🚀 How to Run

```bash
# Start everything
docker compose up --build

# Full reset (deletes all data)
docker compose down -v
docker compose up --build

# Run evaluation tests
docker exec crewai_fraud_detector python test_evaluation.py

# View agent logs
docker logs -f crewai_fraud_detector

# View traces
open http://localhost:6006

# View Kafka
open http://localhost:8080

# View database
open http://localhost:8081
```

---

## 🗄️ Database Schema

```sql
-- Transactions
transactions (tx_id, user_id, amount, location, merchant, merchant_category, timestamp)

-- User profiles
user_profiles (user_id, country_of_residence, has_travel_notice)

-- Analysis results
fraud_analysis_results (tx_id, risk_score, decision, analysis_text, analyzed_at)

-- Agent metrics
agent_metrics (tx_id, elapsed_seconds, total_tokens, decision, created_at)
```

---

## 📦 Tech Stack

- **Agents:** CrewAI >=0.28.8
- **Local LLM:** Ollama + qwen2.5:7b
- **Streaming:** Apache Kafka 7.5.0
- **Database:** PostgreSQL 15
- **Observability:** Arize Phoenix + OpenTelemetry
- **Infrastructure:** Docker Compose
- **GPU:** NVIDIA (compute capability required)