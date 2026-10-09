{{ config(
    materialized = 'view',
    tags = ['silver', 'staging'],
    description = 'SILVER: Datos limpios, deduplicados, enriquecidos y validados'
) }}

-- CAPA SILVER: LIMPIEZA, TRANSFORMACIÓN Y ENRIQUECIMIENTO
-- Elimina duplicados, enriquece datos y prepara para uso analítico

WITH bronze_data AS (
    SELECT *
    FROM {{ ref('stg_raw_crypto_prices') }}
    WHERE data_quality_flag = 'VALID'  -- Solo datos válidos de BRONZE
),

enrich_data AS (
    SELECT
        -- Identifiers
        coin_id,
        symbol,
        name,

        -- Precios y capitales
        current_price,
        market_cap,
        market_cap_rank,
        price_change_24h_pct,

        -- Enriquecimiento: Categorías
        CASE
            WHEN market_cap_rank <= 10 THEN 'Top 10'
            WHEN market_cap_rank <= 50 THEN 'Top 50'
            WHEN market_cap_rank <= 100 THEN 'Top 100'
            ELSE 'Other'
        END as market_cap_category,

        -- Enriquecimiento: Sentimiento de precio
        CASE
            WHEN price_change_24h_pct > 5 THEN 'BULLISH'
            WHEN price_change_24h_pct < -5 THEN 'BEARISH'
            ELSE 'NEUTRAL'
        END as price_sentiment_24h,

        -- Enriquecimiento: Volatilidad
        CASE
            WHEN ABS(price_change_24h_pct) > 10 THEN 'HIGH'
            WHEN ABS(price_change_24h_pct) > 5 THEN 'MEDIUM'
            ELSE 'LOW'
        END as volatility_24h,

        -- Auditoría
        ingested_at,
        bronze_processed_at,
        CURRENT_TIMESTAMP() as silver_processed_at

    FROM bronze_data
),

deduplication AS (
    -- Elimina duplicados: mantiene el registro más reciente por coin_id
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY coin_id
            ORDER BY ingested_at DESC
        ) as rn
    FROM enrich_data
)

SELECT * EXCEPT(rn)
FROM deduplication
WHERE rn = 1

-- Documentación
{% docs stg_crypto_prices %}
Capa SILVER: Datos limpios, validados y enriquecidos.

**Transformaciones:**
- Filtrado: Solo datos válidos de BRONZE
- Deduplicación: 1 registro por coin_id (más reciente)
- Enriquecimiento: Categorías, sentimiento, volatilidad
- Limpieza: Solo datos necesarios para análisis

**Calidad:**
- Todos los registros son válidos
- No hay duplicados
- Datos consistentes y tipados
- Listo para análisis

**Frecuencia:** Diaria (después de BRONZE)
**Grano:** Un registro por criptomoneda
{% enddocs %}
