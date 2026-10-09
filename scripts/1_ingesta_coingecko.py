"""
Ingesta de datos desde CoinGecko API a Databricks
Datos: Precios de criptomonedas en tiempo real (Top 50)
Tabla destino: crypto_data.raw_crypto_prices (BRONZE)

Ejecutar desde: Databricks Notebook o Job
Frecuencia: Diaria a las 02:00 AM UTC
"""

import requests
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
SCHEMA = "crypto_data"
TABLE = "raw_crypto_prices"

# Parámetros de la API
API_PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 50,  # Top 50 criptomonedas
    "page": 1,
    "sparkline": False
}

# ============================================================================
# INICIALIZAR SPARK
# ============================================================================

spark = SparkSession.builder.appName("coingecko-ingest").getOrCreate()

# ============================================================================
# FUNCIONES
# ============================================================================

def fetch_coingecko_data(url: str, params: dict) -> pd.DataFrame:
    """
    Obtiene datos de la API de CoinGecko
    Retorna: DataFrame de Pandas con los datos
    """
    try:
        print(f"\n📡 Obteniendo datos de CoinGecko API...")
        print(f"   URL: {url}")
        print(f"   Parámetros: {params}")

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()
        print(f"✅ Se obtuvieron {len(data)} criptomonedas")

        return pd.DataFrame(data)

    except requests.exceptions.RequestException as e:
        print(f"❌ Error al obtener datos de CoinGecko: {e}")
        raise

def process_coingecko_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa y mapea los datos de CoinGecko al esquema esperado
    """
    try:
        print(f"\n🔄 Procesando datos...")

        processed = []
        for idx, row in df.iterrows():
            processed.append({
                'coin_id': row.get('id'),
                'symbol': row.get('symbol'),
                'name': row.get('name'),
                'current_price': row.get('current_price'),
                'market_cap': row.get('market_cap'),
                'market_cap_rank': row.get('market_cap_rank'),
                'price_change_percentage_24h': row.get('price_change_percentage_24h'),
                'ingested_at': datetime.now()
            })

        result_df = pd.DataFrame(processed)
        print(f"✅ Datos procesados: {len(result_df)} registros")
        return result_df

    except Exception as e:
        print(f"❌ Error procesando datos: {e}")
        raise

def create_schema(schema_name: str):
    """
    Crea el schema si no existe
    """
    try:
        print(f"\n📁 Creando schema '{schema_name}' si no existe...")
        spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {schema_name}
        COMMENT 'Schema para datos de criptomonedas'
        """)
        print(f"✅ Schema '{schema_name}' listo")
    except Exception as e:
        print(f"❌ Error creando schema: {e}")
        raise

def insert_to_databricks(df: pd.DataFrame, schema: str, table: str):
    """
    Inserta datos en Databricks Delta Lake
    Retorna: cantidad de registros insertados
    """
    try:
        print(f"\n💾 Insertando datos en {schema}.{table}...")

        # Convertir a Spark DataFrame
        sdf = spark.createDataFrame(df)

        # Insertar
        sdf.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"{schema}.{table}")

        print(f"✅ {len(df)} registros insertados exitosamente")
        return len(df)

    except Exception as e:
        print(f"❌ Error insertando datos: {e}")
        raise

def validate_ingestion(schema: str, table: str) -> dict:
    """
    Valida que la ingesta fue exitosa
    Retorna: diccionario con estadísticas
    """
    try:
        print(f"\n🔍 Validando ingesta...")

        count = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM {schema}.{table}
        """).collect()[0]["cnt"]

        latest = spark.sql(f"""
        SELECT MAX(ingested_at) as latest FROM {schema}.{table}
        """).collect()[0]["latest"]

        print(f"✅ Total registros en tabla: {count}")
        print(f"✅ Última ingesta: {latest}")

        return {
            "total_records": count,
            "last_ingestion": str(latest),
            "status": "SUCCESS"
        }

    except Exception as e:
        print(f"❌ Error validando: {e}")
        raise

# ============================================================================
# MAIN: EJECUCIÓN DEL PIPELINE
# ============================================================================

if __name__ == "__main__":
    try:
        print("=" * 70)
        print("🚀 PIPELINE DE INGESTA: CoinGecko → Databricks")
        print("=" * 70)

        # 1. Obtener datos de CoinGecko
        raw_data = fetch_coingecko_data(COINGECKO_URL, API_PARAMS)

        # 2. Procesar datos
        processed_data = process_coingecko_data(raw_data)

        # 3. Crear schema
        create_schema(SCHEMA)

        # 4. Insertar en Databricks
        inserted_count = insert_to_databricks(processed_data, SCHEMA, TABLE)

        # 5. Validar ingesta
        validation = validate_ingestion(SCHEMA, TABLE)

        # 6. Mostrar muestra de datos
        print(f"\n📊 Muestra de últimos datos insertados:")
        spark.sql(f"""
        SELECT coin_id, symbol, current_price, market_cap_rank
        FROM {SCHEMA}.{TABLE}
        LIMIT 5
        """).show()

        print("\n" + "=" * 70)
        print("✅ INGESTA COMPLETADA EXITOSAMENTE")
        print("=" * 70)
        print(f"\nResumen:")
        print(f"  - Registros insertados: {inserted_count}")
        print(f"  - Total en tabla: {validation['total_records']}")
        print(f"  - Última actualización: {validation['last_ingestion']}")

    except Exception as e:
        print(f"\n❌ ERROR EN PIPELINE: {e}")
        print("=" * 70)
        raise
