# ELT Pipeline: CoinGecko → Databricks + dbt Cloud

Pipeline ELT completamente automatizado con arquitectura **Medallion** para datos de criptomonedas en tiempo real.

## 📊 Arquitectura

```
CoinGecko API (datos crudos)
        ↓
BRONZE (tipado + validación)
        ↓
SILVER (limpieza + enriquecimiento)
        ↓
GOLD (métricas + agregaciones)
        ↓
BI & Analytics
```

## 🚀 Características

- ✅ **Arquitectura Medallion**: Bronze → Silver → Gold
- ✅ **Tipado de datos**: En capa Bronze
- ✅ **Deduplicación**: En capa Silver
- ✅ **Enriquecimiento**: Categorías, sentimiento, volatilidad
- ✅ **Automatización**: Jobs diarios en dbt Cloud
- ✅ **Validaciones**: Tests y flags de calidad
- ✅ **Performance**: Índices y optimizaciones

## 📁 Estructura de Carpetas

```
elt-databricks-dbt/
├── dbt/
│   ├── dbt_project.yml              # Configuración del proyecto
│   ├── packages.yml                 # Dependencias
│   │
│   ├── models/
│   │   ├── _sources.yml             # Definición de fuentes
│   │   │
│   │   ├── bronze/
│   │   │   └── stg_raw_crypto_prices.sql     # Tipado de datos
│   │   │
│   │   ├── silver/
│   │   │   └── stg_crypto_prices.sql        # Limpieza y enriquecimiento
│   │   │
│   │   └── gold/
│   │       └── fct_crypto_metrics.sql       # Métricas agregadas
│   │
│   └── tests/
│       └── (tests personalizados)
│
├── scripts/
│   └── 1_ingesta_coingecko.py       # Script de ingesta
│
├── .gitignore
└── README.md
```

## 🔄 Flujo de Datos

### 1. Ingesta (02:00 AM UTC)
- Script Python obtiene datos de CoinGecko API
- Top 50 criptomonedas por market cap
- Inserta en `crypto_data.raw_crypto_prices`

### 2. Transformación BRONZE (02:00 AM UTC - después de ingesta)
- **Modelo**: `stg_raw_crypto_prices`
- **Acciones**:
  - Cast de tipos de datos
  - Validación básica (VALID/INVALID)
  - Timestamp de procesamiento
- **Materialización**: VIEW

### 3. Transformación SILVER (03:00 AM UTC)
- **Modelo**: `stg_crypto_prices`
- **Acciones**:
  - Filtrado de datos válidos
  - Deduplicación por coin_id
  - Enriquecimiento: categorías, sentimiento, volatilidad
  - Limpieza de datos
- **Materialización**: VIEW
- **Grano**: 1 registro por criptomoneda

### 4. Transformación GOLD (03:00 AM UTC)
- **Modelo**: `fct_crypto_metrics`
- **Acciones**:
  - Cálculo de rankings y percentiles
  - Clasificación por tamaño (Mega/Large/Mid/Small Cap)
  - Agregaciones finales
- **Materialización**: TABLE (optimizado para queries)
- **Índices**: market_cap_rank, symbol

### 5. Tests (03:00 AM UTC - después de transformaciones)
- Validación de unicidad
- Validación de NOT NULL
- Verificación de integridad referencial

## 📅 Schedule

| Hora | Tarea | Descripción |
|------|-------|-------------|
| **02:00 AM UTC** | Ingesta + BRONZE | Obtiene datos de API, tipado y validación |
| **03:00 AM UTC** | SILVER + GOLD | Transformaciones y agregaciones |
| **04:00 AM UTC** | Tests | Validación de calidad de datos |

## 🛠️ Instalación

### 1. Clonar/Crear el Repositorio

```bash
git clone https://github.com/tu-usuario/elt-databricks-dbt.git
cd elt-databricks-dbt
```

### 2. Crear la Estructura de Carpetas

```bash
mkdir -p dbt/models/{bronze,silver,gold,tests}
mkdir -p scripts
```

### 3. Copiar los Archivos

Copia los archivos `.sql`, `.yml` y `.py` en sus carpetas correspondientes:
- `dbt_project.yml` → `dbt/`
- `packages.yml` → `dbt/`
- `_sources.yml` → `dbt/models/`
- `stg_raw_crypto_prices.sql` → `dbt/models/bronze/`
- `stg_crypto_prices.sql` → `dbt/models/silver/`
- `fct_crypto_metrics.sql` → `dbt/models/gold/`
- `1_ingesta_coingecko.py` → `scripts/`
- `.gitignore` → raíz del proyecto

### 4. Crear README

Crea un `README.md` en la raíz (este archivo).

### 5. Git - Commit Inicial

```bash
git add .
git commit -m "Initial ELT setup: CoinGecko → Databricks + dbt"
git push origin main
```

## 🔌 Configurar dbt Cloud

### 1. Crear Proyecto en dbt Cloud

- Nombre: `crypto_elt`
- Conectar GitHub repo: `elt-databricks-dbt`
- ruta: `dbt/`

### 2. Crear Connection a Databricks

En dbt Cloud → Account Settings → Connections:
- **Type**: Databricks
- **Server hostname**: `[tu-workspace].cloud.databricks.com`
- **HTTP path**: `/sql/1.0/warehouses/[warehouse-id]`
- **Token**: [tu PAT token]
- **Catalog**: `hive_metastore`
- **Schema**: `crypto_data`

### 3. Crear Environments

**Development**:
- Type: Development
- Connection: databricks-connection

**Production**:
- Type: Deployment
- Connection: databricks-connection

### 4. Crear Jobs

**Job 1: dbt-run-production**
- Environment: Production
- Commands: `dbt run --select state:modified+` y `dbt test`
- Schedule: Daily at 02:00 AM UTC

**Job 2: dbt-test-production**
- Environment: Production
- Commands: `dbt test`
- Schedule: Daily at 03:00 AM UTC

## 📊 Modelos y Descripciones

### stg_raw_crypto_prices (BRONZE)
```sql
-- Tipado y validación básica
SELECT 
  coin_id, symbol, name,
  current_price, market_cap, market_cap_rank,
  data_quality_flag
FROM raw_crypto_prices
```

### stg_crypto_prices (SILVER)
```sql
-- Limpio, deduplicado y enriquecido
SELECT 
  coin_id, symbol, name,
  current_price, market_cap, market_cap_rank,
  market_cap_category, price_sentiment_24h, volatility_24h
FROM stg_crypto_prices
```

### fct_crypto_metrics (GOLD)
```sql
-- Métricas agregadas para análisis
SELECT 
  coin_id, symbol, name,
  price_usd, market_cap_billions, 
  market_category, global_rank,
  price_sentiment_24h, volatility_24h
FROM fct_crypto_metrics
```

## 🔍 Validación

Para validar que todo funciona:

```bash
# En dbt Cloud o localmente:
dbt run
dbt test

# Ver documentación:
dbt docs generate
dbt docs serve
```

## 📈 Métricas en GOLD

La tabla `fct_crypto_metrics` contiene:
- Precios en USD
- Market cap en billones
- Rankings globales y por precio
- Sentimiento de precio (BULLISH/NEUTRAL/BEARISH)
- Volatilidad (HIGH/MEDIUM/LOW)
- Categorización por market cap

## 🚨 Monitoreo

Monitorea en dbt Cloud:
- **Run history**: Ver ejecuciones diarias
- **Logs**: Depurar fallos
- **Docs**: Documentación automática
- **Tests**: Estado de validaciones

## 📝 Próximos Pasos

- [ ] Agregar más APIs (stocks, forex, etc)
- [ ] Crear tests personalizados
- [ ] Documentar en dbt Docs
- [ ] Configurar alertas
- [ ] Crear dashboards
- [ ] Migrar a Databricks Premium si es necesario

## 📞 Soporte

- **dbt Docs**: https://docs.getdbt.com
- **Databricks Docs**: https://docs.databricks.com
- **dbt Cloud Docs**: https://docs.getdbt.com/docs/cloud
- **CoinGecko API**: https://www.coingecko.com/api/documentations/v3

---

**Creado**: 2026-10-09  
**Stack**: CoinGecko API → Python → Databricks → dbt Cloud  
**Arquitectura**: Medallion (Bronze → Silver → Gold)  
**Automatización**: dbt Cloud Jobs (diariamente)
