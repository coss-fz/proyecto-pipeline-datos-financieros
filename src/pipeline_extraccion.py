"""
Pipeline ETL principal: Extracción → Validación → Transformación → Carga
"""

from pathlib import Path
import logging
import sqlite3
import pandas as pd




logger = logging.getLogger("extraccion")

RAW_DIR = Path("data/raw")
DB_PATH = Path("data/innova_finance.db")




def extraer_datos() -> dict[str, pd.DataFrame]:
    """Lee todos los CSV crudos y los retorna como un diccionario de DataFrames"""
    archivos = {
        "transacciones":    "transactions.csv",
        "pagos":            "payments.csv",
        "gastos":           "expenses.csv",
        "clientes":         "customers.csv",
        "empleados":        "employees.csv",
        "suscripciones":    "subscriptions.csv",
    }
    datos: dict[str, pd.DataFrame] = {}

    if not RAW_DIR.exists():
        logger.error("Error crítico: El directorio '%s' no existe.", RAW_DIR)
        raise FileNotFoundError(f"No se encontró la carpeta de origen: {RAW_DIR}")

    for nombre, archivo in archivos.items():
        ruta = RAW_DIR / archivo
        logger.info("Leyendo %s...", ruta)
        if not ruta.exists():
            logger.error("Archivo faltante: %s", ruta)
            raise FileNotFoundError(f"Falta el archivo requerido: {archivo}")
        df = pd.read_csv(ruta)
        logger.info("  → %d filas, %d columnas", len(df), len(df.columns))
        datos[nombre] = df
    return datos


def validar_datos(datos: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Aplica reglas básicas de calidad:
    - Detecta y registra nulos
    - Elimina duplicados
    - Normaliza tipos de fecha
    - Elimina montos negativos en transacciones, pagos y gastos
    """
    columnas_fecha = {
        "transacciones":    ["date"],
        "pagos":            ["payment_date"],
        "gastos":           ["date"],
        "clientes":         ["registration_date"],
        "empleados":        ["hire_date"],
        "suscripciones":    ["start_date", "end_date"],
    }
    columnas_monto_positivo = {
        "transacciones":    ["total_usd", "unit_price_usd", "quantity"],
        "pagos":            ["amount_usd"],
        "gastos":           ["amount_usd"],
        "empleados":        ["salary_usd"],
        "suscripciones":    ["monthly_price_usd"],
    }

    for nombre, df in datos.items():
        nulos = df.isnull().sum()
        nulos_existentes = nulos[nulos > 0]
        if not nulos_existentes.empty:
            logger.warning("[%s] Nulos detectados:\n%d", nombre, nulos_existentes)

        filas_antes = len(df)
        df = df.drop_duplicates()
        duplicados = filas_antes - len(df)
        if duplicados:
            logger.warning("[%s] Se eliminaron %d filas duplicadas.", nombre, duplicados)

        for col in columnas_fecha.get(nombre, []):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        for col in columnas_monto_positivo.get(nombre, []):
            if col in df.columns:
                negativos = (df[col] < 0).sum()
                if negativos:
                    logger.warning(
                        "[%s] %d valores negativos en '%s' → eliminados", nombre, negativos, col
                    )
                    df = df[df[col] >= 0]

        datos[nombre] = df
        logger.info("[%s] Validación completada: %d filas", nombre, len(df))

    return datos


def cargar_datos(datos: dict[str, pd.DataFrame]) -> None: # ERROR HANDLING
    """Carga los DataFrames validados en SQLite como tablas de información cruda"""
    con = None
    tabla_map = {
        "transacciones":    "raw_transacciones",
        "pagos":            "raw_pagos",
        "gastos":           "raw_gastos",
        "clientes":         "raw_clientes",
        "empleados":        "raw_empleados",
        "suscripciones":    "raw_suscripciones",
    }
    try:
        con = sqlite3.connect(DB_PATH)
        for nombre, df in datos.items():
            tabla = tabla_map[nombre]
            df.to_sql(tabla, con, if_exists="replace", index=False)
            logger.info("Tabla '%s' cargada: %d filas.", tabla, len(df))
    except sqlite3.Error as e:
        logger.error("Error de base de datos: %s", e)
        raise
    finally:
        if con:
            con.close()
            logger.info("Base de datos guardada en: '%s", DB_PATH)


def ejecutar_pipeline() -> None: # pragma: no cover
    """Ejecución de todo el flujo de extracción"""
    dict_datos = extraer_datos()
    dict_datos = validar_datos(dict_datos)
    cargar_datos(dict_datos)
