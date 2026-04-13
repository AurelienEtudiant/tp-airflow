-- =============================================================================
-- Init Source DB PostgreSQL - Formation Airflow IPSSI (Sujet B)
-- Tables : events, users
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS source;

CREATE TABLE IF NOT EXISTS source.users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(200) NOT NULL,
    city VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS source.events (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES source.users(id),
    event_type VARCHAR(50) NOT NULL,
    event_data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Seed users
INSERT INTO source.users (username, email, city, active) VALUES
('alice', 'alice@example.com', 'Paris', TRUE),
('bob', 'bob@example.com', 'Lyon', TRUE),
('charlie', 'charlie@example.com', 'Marseille', TRUE),
('diana', 'diana@example.com', 'Bordeaux', FALSE),
('eve', 'eve@example.com', 'Lille', TRUE),
('frank', 'frank@example.com', 'Toulouse', TRUE),
('grace', 'grace@example.com', 'Nantes', TRUE),
('henry', 'henry@example.com', 'Strasbourg', FALSE),
('iris', 'iris@example.com', 'Nice', TRUE),
('jack', 'jack@example.com', 'Rennes', TRUE),
('kate', 'kate@example.com', 'Paris', TRUE),
('leo', 'leo@example.com', 'Lyon', TRUE),
('maria', 'maria@example.com', 'Marseille', TRUE),
('noel', 'noel@example.com', 'Bordeaux', TRUE),
('olivia', 'olivia@example.com', 'Lille', FALSE),
('paul', 'paul@example.com', 'Toulouse', TRUE),
('quinn', 'quinn@example.com', 'Nantes', TRUE),
('rosa', 'rosa@example.com', 'Strasbourg', TRUE),
('sam', 'sam@example.com', 'Nice', TRUE),
('tina', 'tina@example.com', 'Rennes', TRUE);

-- Seed events (100 lignes)
INSERT INTO source.events (user_id, event_type, event_data, created_at)
SELECT
    (1 + (g % 20)),
    (ARRAY['page_view', 'click', 'purchase', 'signup', 'logout'])[1 + (g % 5)],
    json_build_object(
        'page', '/page-' || (g % 10),
        'duration_ms', 100 + (g * 37 % 5000),
        'device', (ARRAY['mobile', 'desktop', 'tablet'])[1 + (g % 3)]
    )::jsonb,
    NOW() - (g || ' hours')::interval
FROM generate_series(1, 100) AS g;
