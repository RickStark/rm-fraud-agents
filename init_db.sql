-- ==========================================
-- Script de inicialización de PostgreSQL
-- Base de datos: fraud_detection
-- ==========================================

-- Tabla de perfiles de usuario
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id VARCHAR(50) PRIMARY KEY,
    country_of_residence VARCHAR(100),
    has_travel_notice BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Tabla de transacciones
CREATE TABLE IF NOT EXISTS transactions (
    id SERIAL PRIMARY KEY,
    tx_id VARCHAR(50) UNIQUE NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    location VARCHAR(200),
    merchant VARCHAR(200),
    merchant_category VARCHAR(100),
    timestamp TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (user_id) REFERENCES user_profiles(user_id)
);

-- Tabla de resultados de análisis de fraude
CREATE TABLE IF NOT EXISTS fraud_analysis_results (
    id SERIAL PRIMARY KEY,
    tx_id VARCHAR(50) NOT NULL,
    risk_score INTEGER,
    decision VARCHAR(20),
    analysis_text TEXT,
    analyzed_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (tx_id) REFERENCES transactions(tx_id)
);

-- Índices para mejorar el rendimiento
CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions(user_id);
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_tx_id ON transactions(tx_id);
CREATE INDEX IF NOT EXISTS idx_fraud_results_tx_id ON fraud_analysis_results(tx_id);

-- ==========================================
-- Datos de ejemplo para pruebas
-- ==========================================

-- Insertar usuario de ejemplo
INSERT INTO user_profiles (user_id, country_of_residence, has_travel_notice) 
VALUES ('USR_105', 'Lima, Peru', FALSE)
ON CONFLICT (user_id) DO NOTHING;

INSERT INTO user_profiles (user_id, country_of_residence, has_travel_notice) 
VALUES ('USR_106', 'Buenos Aires, Argentina', TRUE)
ON CONFLICT (user_id) DO NOTHING;

-- Insertar transacciones históricas para USR_105 (últimos 30 días)
INSERT INTO transactions (tx_id, user_id, amount, location, merchant, merchant_category, timestamp)
VALUES 
    ('TX_1001', 'USR_105', 120.50, 'Lima, Peru', 'Supermercados Wong', 'Groceries', NOW() - INTERVAL '25 days'),
    ('TX_1002', 'USR_105', 45.00, 'Lima, Peru', 'Restaurant Central', 'Restaurants', NOW() - INTERVAL '24 days'),
    ('TX_1003', 'USR_105', 180.00, 'Lima, Peru', 'Plaza Vea', 'Groceries', NOW() - INTERVAL '20 days'),
    ('TX_1004', 'USR_105', 15.99, 'Lima, Peru', 'Netflix', 'Streaming', NOW() - INTERVAL '18 days'),
    ('TX_1005', 'USR_105', 200.00, 'Lima, Peru', 'Tottus', 'Groceries', NOW() - INTERVAL '15 days'),
    ('TX_1006', 'USR_105', 85.50, 'Lima, Peru', 'La Rosa Nautica', 'Restaurants', NOW() - INTERVAL '12 days'),
    ('TX_1007', 'USR_105', 130.00, 'Lima, Peru', 'Metro', 'Groceries', NOW() - INTERVAL '10 days'),
    ('TX_1008', 'USR_105', 12.99, 'Lima, Peru', 'Spotify', 'Streaming', NOW() - INTERVAL '8 days'),
    ('TX_1009', 'USR_105', 95.00, 'Lima, Peru', 'Astrid y Gaston', 'Restaurants', NOW() - INTERVAL '5 days'),
    ('TX_1010', 'USR_105', 160.00, 'Lima, Peru', 'Vivanda', 'Groceries', NOW() - INTERVAL '3 days')
ON CONFLICT (tx_id) DO NOTHING;

-- Insertar la transacción sospechosa (esta será enviada por Kafka)
INSERT INTO transactions (tx_id, user_id, amount, location, merchant, merchant_category, timestamp)
VALUES ('TX_9982', 'USR_105', 2500.00, 'Moscú, Rusia', 'CryptoExchange Ltd.', 'Crypto', NOW())
ON CONFLICT (tx_id) DO NOTHING;

-- Transacciones para USR_106 (usuario con aviso de viaje)
INSERT INTO transactions (tx_id, user_id, amount, location, merchant, merchant_category, timestamp)
VALUES 
    ('TX_2001', 'USR_106', 300.00, 'Buenos Aires, Argentina', 'Carrefour', 'Groceries', NOW() - INTERVAL '20 days'),
    ('TX_2002', 'USR_106', 450.00, 'Madrid, España', 'El Corte Inglés', 'Shopping', NOW() - INTERVAL '10 days'),
    ('TX_2003', 'USR_106', 280.00, 'Barcelona, España', 'Restaurante La Paradeta', 'Restaurants', NOW() - INTERVAL '5 days')
ON CONFLICT (tx_id) DO NOTHING;

-- Vista para análisis rápido
CREATE OR REPLACE VIEW v_recent_fraud_analysis AS
SELECT 
    t.tx_id,
    t.user_id,
    t.amount,
    t.location,
    t.merchant,
    t.timestamp as tx_timestamp,
    far.risk_score,
    far.decision,
    far.analyzed_at
FROM transactions t
LEFT JOIN fraud_analysis_results far ON t.tx_id = far.tx_id
ORDER BY t.timestamp DESC;

COMMENT ON TABLE user_profiles IS 'Perfiles de usuarios con información de residencia y avisos de viaje';
COMMENT ON TABLE transactions IS 'Registro de todas las transacciones financieras';
COMMENT ON TABLE fraud_analysis_results IS 'Resultados del análisis de fraude realizado por los agentes de IA';
