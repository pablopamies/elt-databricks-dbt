{{ config(
    materialized = 'table',
    tags = ['gold', 'marts'],
    description = 'GOLD: Tabla de hechos con métricas agregadas de criptomonedas',
    indexes = [
        {'columns': ['market_cap_rank'], 'type': 'btree'},
        {'columns': ['symbol'], 'type': 'btree'}
    ]
) }}

-- CAPA GOLD: TABLA DE HECHOS CON MÉTRICAS
-- Agregaciones y métricas listas para BI y análisis

WITH silver_data AS (
    SELECT * FROM {{ ref('stg_crypto_prices') }}
)

SELECT
    -- Dimensiones
    coin_id,
    symbol,
    name,
    market_cap_rank,
    market_cap_category,

    -- Métricas financieras
    ROUND(current_price, 2) as price_usd,
    ROUND(market_cap / 1000000000, 2) as market_cap_billions,
    ROUND(price_change_24h_pct, 2) as price_change_24h_pct,

    -- Análisis y sentimiento
    price_sentiment_24h,
    volatility_24h,

    -- Rankings y percentiles
    ROW_NUMBER() OVER (ORDER BY market_cap_rank) as global_rank,
    RANK() OVER (ORDER BY current_price DESC) as price_rank,
    PERCENT_RANK() OVER (ORDER BY market_cap_rank) as market_cap_percentile,

    -- Clasificación por tamaño
    CASE
        WHEN market_cap_rank <= 5 THEN 'Mega Cap'
        WHEN market_cap_rank <= 20 THEN 'Large Cap'
        WHEN market_cap_rank <= 100 THEN 'Mid Cap'
        ELSE 'Small Cap'
    END as market_category,

    -- Auditoría y tracking
    CURRENT_DATE() as metric_date,
    CURRENT_TIMESTAMP() as last_updated,
    ingested_at as source_ingested_at

FROM silver_data

ORDER BY market_cap_rank ASC

-- Documentación
{% docs fct_crypto_metrics %}
Tabla de HECHOS GOLD: Métricas de criptomonedas para análisis y BI.

**Contenido:**
- Identifiers: coin_id, symbol, name
- Precios: price_usd, market_cap_billions
- Cambios: price_change_24h_pct, price_sentiment_24h
- Volatilidad: volatility_24h, market_category
- Rankings: global_rank, price_rank, market_cap_percentile

**Características:**
- Materialización: TABLE (optimizado para queries)
- Grano: Un registro por criptomoneda
- Actualización: Diaria
- Índices: market_cap_rank, symbol para performance
- Listo para dashboards y BI

**Casos de uso:**
- Dashboards de precios en tiempo real
- Análisis de mercado y tendencias
- Comparativas entre criptomonedas
- Tracking de volatilidad
- Alertas de cambios de precio

**Frecuencia:** Diaria (después de SILVER)
{% enddocs %}
