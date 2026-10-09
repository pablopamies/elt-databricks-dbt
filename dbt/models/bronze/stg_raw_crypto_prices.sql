{{ config(
    materialized = 'view',
    tags = ['bronze', 'raw'],
    description = 'BRONZE: Tipado y validación básica de datos crudos de CoinGecko'
) }}

-- CAPA BRONZE: TIPADO DE DATOS Y VALIDACIÓN BÁSICA
-- Aplica tipos de datos correctos y validaciones básicas sobre datos RAW

SELECT
    -- IDs y identificadores (STRING)
    CAST(coin_id AS STRING) as coin_id,
    CAST(UPPER(symbol) AS STRING) as symbol,
    CAST(name AS STRING) as name,

    -- Números financieros (tipado correcto)
    CAST(current_price AS DECIMAL(18, 8)) as current_price,
    CAST(market_cap AS DECIMAL(20, 2)) as market_cap,
    CAST(market_cap_rank AS INT) as market_cap_rank,
    CAST(price_change_percentage_24h AS DECIMAL(10, 2)) as price_change_24h_pct,

    -- Datos de auditoría
    CAST(ingested_at AS TIMESTAMP) as ingested_at,
    CURRENT_TIMESTAMP() as bronze_processed_at,

    -- Flag de validación básica
    CASE
        WHEN coin_id IS NULL THEN 'INVALID_NO_ID'
        WHEN symbol IS NULL THEN 'INVALID_NO_SYMBOL'
        WHEN current_price IS NULL THEN 'INVALID_NO_PRICE'
        WHEN market_cap_rank IS NULL THEN 'INVALID_NO_RANK'
        ELSE 'VALID'
    END as data_quality_flag

FROM {{ source('coingecko_api', 'raw_crypto_prices') }}

WHERE ingested_at IS NOT NULL

-- Documentación
{% docs stg_raw_crypto_prices %}
Capa BRONZE: Primera transformación de datos crudos.

**Transformaciones:**
- Cast de tipos de datos a tipos correctos
- Normalización de símbolos (UPPER)
- Validación básica de campos obligatorios

**Calidad:**
- Flag de validación (VALID/INVALID)
- Timestamp de procesamiento en BRONZE
- Preserva todos los datos RAW

**Frecuencia:** Diaria (después de ingesta)
{% enddocs %}
