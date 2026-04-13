-- ============================================================
-- NewsRadar - Schéma PostgreSQL
-- Exécuté automatiquement au démarrage de postgres-newsradar
-- ============================================================

-- Extension UUID
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================
-- DIM_SOURCE : catalogue des sources d'articles
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_source (
    source_id   VARCHAR(100)    PRIMARY KEY,
    name        VARCHAR(200)    NOT NULL,
    type        VARCHAR(20)     NOT NULL CHECK (type IN ('rss', 'api', 'html')),
    url         TEXT            NOT NULL,
    is_active   BOOLEAN         DEFAULT TRUE,
    created_at  TIMESTAMP       DEFAULT NOW(),
    updated_at  TIMESTAMP       DEFAULT NOW()
);

COMMENT ON TABLE  dim_source IS 'Catalogue des sources d''articles (RSS, API, HTML)';
COMMENT ON COLUMN dim_source.type IS 'Type de source : rss | api | html';

-- ============================================================
-- FACT_ARTICLE : articles ingérés, avec déduplication
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_article (
    article_id      UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id       VARCHAR(100)    REFERENCES dim_source(source_id),
    url             TEXT            NOT NULL,
    url_hash        VARCHAR(64)     NOT NULL,   -- sha256(url.strip().lower())
    title           TEXT            NOT NULL,
    title_hash      VARCHAR(64)     NOT NULL,   -- sha256(title.strip().lower())
    body            TEXT,
    published_at    TIMESTAMP,
    s3_path         TEXT,                       -- chemin MinIO : raw/{type}/dt=YYYY-MM-DD/{source_id}.json
    ingested_at     TIMESTAMP       DEFAULT NOW(),
    enriched        BOOLEAN         DEFAULT FALSE,
    indexed         BOOLEAN         DEFAULT FALSE,  -- true quand indexé dans OpenSearch
    CONSTRAINT uq_url_hash UNIQUE (url_hash)
);

-- Index pour la déduplication (obligatoire, sinon lent sur 8k articles/jour)
CREATE INDEX IF NOT EXISTS idx_fact_article_url_hash    ON fact_article(url_hash);
CREATE INDEX IF NOT EXISTS idx_fact_article_title_hash  ON fact_article(title_hash);
CREATE INDEX IF NOT EXISTS idx_fact_article_enriched    ON fact_article(enriched) WHERE enriched = FALSE;
CREATE INDEX IF NOT EXISTS idx_fact_article_indexed     ON fact_article(indexed)  WHERE indexed = FALSE;
CREATE INDEX IF NOT EXISTS idx_fact_article_source      ON fact_article(source_id);
CREATE INDEX IF NOT EXISTS idx_fact_article_published   ON fact_article(published_at DESC);

COMMENT ON TABLE  fact_article IS 'Articles ingérés depuis toutes les sources. Déduplication par url_hash.';
COMMENT ON COLUMN fact_article.url_hash   IS 'sha256(url.strip().lower()) — contrainte UNIQUE';
COMMENT ON COLUMN fact_article.title_hash IS 'sha256(title.strip().lower()) — détection doublons titre';
COMMENT ON COLUMN fact_article.s3_path    IS 'Chemin MinIO de l''article brut. Format : raw/{type}/dt=YYYY-MM-DD/{source_id}/{article_id}.json';
COMMENT ON COLUMN fact_article.enriched   IS 'True quand l''enrichissement ML (topic + sentiment) est fait';
COMMENT ON COLUMN fact_article.indexed    IS 'True quand l''article est indexé dans OpenSearch';

-- ============================================================
-- FACT_ENRICHMENT : résultats ML (topic, sentiment, langue)
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_enrichment (
    article_id          UUID        PRIMARY KEY REFERENCES fact_article(article_id) ON DELETE CASCADE,
    topic               VARCHAR(50),
    topic_confidence    FLOAT       CHECK (topic_confidence BETWEEN 0.0 AND 1.0),
    sentiment_label     VARCHAR(20) CHECK (sentiment_label IN ('POSITIVE', 'NEGATIVE', 'NEUTRAL')),
    sentiment_score     FLOAT       CHECK (sentiment_score BETWEEN 0.0 AND 1.0),
    language            VARCHAR(10) DEFAULT 'unknown',
    word_count          INTEGER,
    enriched_at         TIMESTAMP   DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_enrichment_topic     ON fact_enrichment(topic);
CREATE INDEX IF NOT EXISTS idx_fact_enrichment_sentiment ON fact_enrichment(sentiment_label);

COMMENT ON TABLE  fact_enrichment IS 'Résultats d''enrichissement ML par article (NLP service)';
COMMENT ON COLUMN fact_enrichment.topic             IS 'Topic classifié : tech | politics | business | sports | culture | science | health';
COMMENT ON COLUMN fact_enrichment.topic_confidence  IS 'Score de confiance du classificateur zero-shot (0.0-1.0)';
COMMENT ON COLUMN fact_enrichment.sentiment_label   IS 'Sentiment : POSITIVE | NEGATIVE | NEUTRAL';
COMMENT ON COLUMN fact_enrichment.sentiment_score   IS 'Score de confiance du modèle de sentiment (0.0-1.0)';

-- ============================================================
-- FACT_INGEST_LOG : log des exécutions d'ingestion par source
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_ingest_log (
    log_id          SERIAL          PRIMARY KEY,
    source_id       VARCHAR(100)    REFERENCES dim_source(source_id),
    run_date        DATE            NOT NULL,
    articles_found  INTEGER         DEFAULT 0,
    articles_new    INTEGER         DEFAULT 0,
    articles_dup    INTEGER         DEFAULT 0,
    status          VARCHAR(20)     DEFAULT 'success' CHECK (status IN ('success', 'error', 'empty')),
    error_message   TEXT,
    started_at      TIMESTAMP       DEFAULT NOW(),
    finished_at     TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ingest_log_source   ON fact_ingest_log(source_id);
CREATE INDEX IF NOT EXISTS idx_ingest_log_run_date ON fact_ingest_log(run_date DESC);
CREATE INDEX IF NOT EXISTS idx_ingest_log_status   ON fact_ingest_log(status);

COMMENT ON TABLE fact_ingest_log IS 'Journal des ingestions. Permet de détecter les sources en échec (US-05).';

-- ============================================================
-- SEED : sources initiales (fixtures fournies dans le kit)
-- ============================================================
INSERT INTO dim_source (source_id, name, type, url, is_active) VALUES
    ('rss_lemonde',     'Le Monde',         'rss', 'fixtures/rss/lemonde.xml',     TRUE),
    ('rss_reuters',     'Reuters',          'rss', 'fixtures/rss/reuters.xml',     TRUE),
    ('rss_techcrunch',  'TechCrunch',       'rss', 'fixtures/rss/techcrunch.xml',  TRUE),
    ('rss_lefigaro',    'Le Figaro',        'rss', 'fixtures/rss/lefigaro.xml',    TRUE),
    ('rss_bbc',         'BBC News',         'rss', 'fixtures/rss/bbc.xml',         TRUE),
    ('api_news',        'News API Simulée', 'api', 'http://api-news-simulee:5500', TRUE),
    ('html_fixtures',   'HTML Fixtures',    'html','fixtures/html/',               TRUE)
ON CONFLICT (source_id) DO NOTHING;

-- ============================================================
-- VUE : stats rapides pour monitoring
-- ============================================================
CREATE OR REPLACE VIEW v_ingest_stats AS
SELECT
    s.source_id,
    s.name,
    s.type,
    COUNT(a.article_id)                                         AS total_articles,
    COUNT(a.article_id) FILTER (WHERE a.enriched = TRUE)        AS enriched_articles,
    COUNT(a.article_id) FILTER (WHERE a.indexed = TRUE)         AS indexed_articles,
    MAX(a.ingested_at)                                          AS last_ingestion
FROM dim_source s
LEFT JOIN fact_article a ON s.source_id = a.source_id
GROUP BY s.source_id, s.name, s.type;

COMMENT ON VIEW v_ingest_stats IS 'Vue de monitoring : avancement ingestion/enrichissement/indexation par source';

-- ============================================================
-- VUE : articles prêts à enrichir (batch ML)
-- ============================================================
CREATE OR REPLACE VIEW v_articles_to_enrich AS
SELECT
    article_id,
    source_id,
    title,
    body,
    published_at
FROM fact_article
WHERE enriched = FALSE
  AND body IS NOT NULL
  AND LENGTH(body) > 50
ORDER BY ingested_at ASC;

-- ============================================================
-- VUE : articles prêts à indexer dans OpenSearch
-- ============================================================
CREATE OR REPLACE VIEW v_articles_to_index AS
SELECT
    a.article_id,
    a.source_id,
    a.url,
    a.title,
    a.body,
    a.published_at,
    e.topic,
    e.topic_confidence,
    e.sentiment_label,
    e.sentiment_score,
    e.language
FROM fact_article a
JOIN fact_enrichment e ON a.article_id = e.article_id
WHERE a.indexed = FALSE;

-- ============================================================
-- FONCTION UTILITAIRE : vérification doublons
-- ============================================================
CREATE OR REPLACE FUNCTION check_duplicates()
RETURNS TABLE(url_hash VARCHAR(64), cnt BIGINT) AS $$
BEGIN
    RETURN QUERY
    SELECT fa.url_hash, COUNT(*) AS cnt
    FROM fact_article fa
    GROUP BY fa.url_hash
    HAVING COUNT(*) > 1;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION check_duplicates IS 'Vérifie qu''il n''y a aucun doublon. SELECT * FROM check_duplicates() doit retourner 0 lignes.';
