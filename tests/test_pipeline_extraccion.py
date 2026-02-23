"""
Tests para pipeline_extraccion.py
Cubre: extraer_datos, validar_datos, cargar_datos
"""

import sys
from pathlib import Path
import logging
import sqlite3
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from src.pipeline_extraccion import extraer_datos, validar_datos, cargar_datos # pylint: disable=wrong-import-position




pytestmark = pytest.mark.filterwarnings("ignore")

@pytest.fixture
def datos_validos() -> dict[str, pd.DataFrame]:
    """Conjunto mínimo de DataFrames válidos para todas las etapas"""
    return {
        "transacciones": pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02"],
            "total_usd": [100.0, 200.0],
            "unit_price_usd": [10.0, 20.0],
            "quantity": [10, 10],
        }),
        "pagos": pd.DataFrame({
            "payment_date": ["2024-01-01"],
            "amount_usd": [50.0],
        }),
        "gastos": pd.DataFrame({
            "date": ["2024-01-01"],
            "amount_usd": [30.0],
        }),
        "clientes": pd.DataFrame({
            "registration_date": ["2024-01-01"],
            "name": ["Cliente A"],
        }),
        "empleados": pd.DataFrame({
            "hire_date": ["2024-01-01"],
            "salary_usd": [3000.0],
        }),
        "suscripciones": pd.DataFrame({
            "start_date": ["2024-01-01"],
            "end_date": ["2024-12-31"],
            "monthly_price_usd": [99.0],
        }),
    }




class TestExtraerDatos:
    """Clase para definir todos los tests de la función 'extraer_datos"""

    def test_retorna_todos_los_dataframes(self, tmp_path, monkeypatch):
        """Debe devolver un dict con las 6 claves esperadas"""
        monkeypatch.setattr("src.pipeline_extraccion.RAW_DIR", tmp_path)

        archivos = [
            "transactions.csv", "payments.csv", "expenses.csv",
            "customers.csv", "employees.csv", "subscriptions.csv",
        ]
        for nombre in archivos:
            (tmp_path / nombre).write_text("col1,col2\n1,2\n3,4\n")

        datos = extraer_datos()

        assert set(datos.keys()) == {
            "transacciones", "pagos", "gastos",
            "clientes", "empleados", "suscripciones",
        }
        for df in datos.values():
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 2


    def test_error_si_directorio_no_existe(self, tmp_path, monkeypatch):
        """Debe lanzar FileNotFoundError si RAW_DIR no existe"""
        monkeypatch.setattr(
            "src.pipeline_extraccion.RAW_DIR", tmp_path / "no_existe"
        )
        with pytest.raises(FileNotFoundError, match="carpeta de origen"):
            extraer_datos()


    def test_error_si_archivo_faltante(self, tmp_path, monkeypatch):
        """Debe lanzar FileNotFoundError si falta algún CSV"""
        monkeypatch.setattr("src.pipeline_extraccion.RAW_DIR", tmp_path)
        for nombre in ["transactions.csv", "payments.csv", "expenses.csv",
                       "customers.csv", "employees.csv"]:
            (tmp_path / nombre).write_text("col1\n1\n")
        with pytest.raises(FileNotFoundError, match="subscriptions.csv"):
            extraer_datos()




class TestValidarDatos:
    """Clase para definir todos los tests de la función 'validar_datos'"""

    def test_warning_nulos(self, datos_validos, caplog): # pylint: disable=redefined-outer-name
        """Debe emitir un warning de log cuando se detectan valores nulos"""
        datos_validos["clientes"] = pd.DataFrame({
            "registration_date": ["2024-01-01", None],
            "name": ["Cliente A", None],
        })
        with caplog.at_level(logging.WARNING, logger="extraccion"):
            validar_datos(datos_validos)
        assert any("Nulos detectados" in msg for msg in caplog.messages)


    def test_elimina_duplicados(self, datos_validos): # pylint: disable=redefined-outer-name
        """Filas duplicadas deben eliminarse"""
        df_dup = pd.concat(
            [datos_validos["pagos"], datos_validos["pagos"]], ignore_index=True
        )
        datos_validos["pagos"] = df_dup.copy()
        resultado = validar_datos(datos_validos)
        assert len(resultado["pagos"]) == 1


    def test_convierte_columnas_fecha(self, datos_validos): # pylint: disable=redefined-outer-name
        """Las columnas de fecha deben quedar como datetime64"""
        resultado = validar_datos(datos_validos)
        assert pd.api.types.is_datetime64_any_dtype(
            resultado["transacciones"]["date"]
        )
        assert pd.api.types.is_datetime64_any_dtype(
            resultado["pagos"]["payment_date"]
        )
        assert pd.api.types.is_datetime64_any_dtype(
            resultado["suscripciones"]["start_date"]
        )
        assert pd.api.types.is_datetime64_any_dtype(
            resultado["suscripciones"]["end_date"]
        )


    def test_elimina_montos_negativos(self, datos_validos): # pylint: disable=redefined-outer-name
        """Filas con total_usd negativo deben eliminarse"""
        datos_validos["transacciones"] = pd.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "total_usd": [100.0, -50.0, 200.0],
            "unit_price_usd": [10.0, 10.0, 20.0],
            "quantity": [10, 5, 10],
        })
        resultado = validar_datos(datos_validos)
        assert (resultado["transacciones"]["total_usd"] >= 0).all()
        assert len(resultado["transacciones"]) == 2




class TestCargarDatos:
    """Clase para definir todos los tests de la función 'cargar_datos'"""

    def test_crea_tablas_en_sqlite(self, datos_validos, tmp_path, monkeypatch): # pylint: disable=redefined-outer-name
        """Debe crear una tabla raw_* por cada DataFrame en la BD SQLite"""
        db = tmp_path / "test.db"
        monkeypatch.setattr("src.pipeline_extraccion.DB_PATH", db)

        cargar_datos(datos_validos)

        con = sqlite3.connect(db)
        tablas = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        con.close()

        esperadas = {
            "raw_transacciones", "raw_pagos", "raw_gastos",
            "raw_clientes", "raw_empleados", "raw_suscripciones",
        }
        assert esperadas == tablas


    def test_propaga_error_sqlite(self, datos_validos, monkeypatch): # pylint: disable=redefined-outer-name
        """Un error de SQLite debe propagarse"""
        monkeypatch.setattr(
            "src.pipeline_extraccion.DB_PATH", Path("/ruta/invalida/db.db")
        )
        with pytest.raises(Exception):
            cargar_datos(datos_validos)
