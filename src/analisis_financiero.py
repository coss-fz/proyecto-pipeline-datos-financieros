"""
Clasificación de gastos y forecast financiero usando Claude (Anthropic API).

Implementación:
- Clasifica gastos por categoría usando reglas + IA
- Genera un forecast mensual con regresión lineal simple (statsmodels/numpy)
- Envía resumen consolidado a Claude para obtener conclusiones clave de negocio
"""

import os
import logging
from pathlib import Path
from datetime import datetime
import sqlite3
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import anthropic




load_dotenv()
logger = logging.getLogger("analisis_financiero")

DB_PATH = Path("data/innova_finance.db")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")




def _leer_vista(nombre_vista: str) -> pd.DataFrame:
    """Lee cualquier vista desde la BD"""
    con = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql(f"SELECT * FROM {nombre_vista}", con)
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.error("Error leyendo %s: %s", nombre_vista, e)
        raise RuntimeError(f"Error leyendo {nombre_vista}: {e}") from e
    finally:
        con.close()
    return df


def _obtener_gastos() -> pd.DataFrame:
    return _leer_vista("vw_gastos_mensuales")


def _obtener_ingresos() -> pd.DataFrame:
    return _leer_vista("vw_ingresos_mensuales")


def _obtener_mrr() -> pd.DataFrame:
    return _leer_vista("vw_mrr_mensual")


def resumen_por_categoria(df: pd.DataFrame) -> pd.DataFrame:
    """Agrupa gasto total y % por categoría"""
    resumen = (
        df.groupby("category")["gastos_usd"]
        .agg(total="sum", transacciones="count")
        .reset_index()
        .sort_values("total", ascending=False)
    )
    resumen["pct"] = (resumen["total"] / resumen["total"].sum() * 100).round(2)
    return resumen


def resumen_mrr_por_plan(df: pd.DataFrame) -> pd.DataFrame:
    """Agrupa MRR total, suscripciones activas y % por plan"""
    resumen = (
        df.groupby("plan")
        .agg(
            mrr_total=("mrr_usd", "sum"),
            suscripciones=("total_suscripciones", "sum"),
        )
        .reset_index()
        .sort_values("mrr_total", ascending=False)
    )
    resumen["pct"] = (resumen["mrr_total"] / resumen["mrr_total"].sum() * 100).round(2)
    return resumen


def _serie_mensual(df: pd.DataFrame, col_monto: str) -> pd.Series:
    """Convierte un DataFrame en una serie mensual de totales"""
    df = df.copy()
    df["periodo"] = pd.to_datetime(
        df["anio"].astype(str) + "-" + df["mes"].astype(str).str.zfill(2)
    )
    serie = df.groupby("periodo")[col_monto].sum().sort_index()
    return serie


def forecast_lineal(serie: pd.Series, meses_adelante: int = 3) -> pd.DataFrame:
    """
    - Forecast simple con regresión lineal (numpy polyfit)
    - Retorna un DataFrame con periodos futuros y valores proyectados
    """
    if len(serie) < 3:
        logger.warning("Serie demasiado corta para forecast (<3 puntos)")
        return pd.DataFrame(columns=["periodo", "forecast_usd"])

    x = np.arange(len(serie))
    y = serie.values.astype(float)

    coef = np.polyfit(x, y, deg=1)
    poly = np.poly1d(coef)

    ultimo_periodo = serie.index[-1]
    periodos_futuros = pd.date_range(
        start=ultimo_periodo + pd.DateOffset(months=1),
        periods=meses_adelante,
        freq="MS",
    )

    x_futuro = np.arange(len(serie), len(serie) + meses_adelante)
    valores_forecast = poly(x_futuro)

    resultado = pd.DataFrame({
        "periodo": periodos_futuros,
        "forecast_usd": np.maximum(valores_forecast, 0).round(2),
    })
    return resultado


def calcular_margen_proyectado(
    forecast_ingresos: pd.DataFrame,
    forecast_gastos: pd.DataFrame,
    forecast_mrr: pd.DataFrame,
) -> pd.DataFrame:
    """
    - Une los tres forecasts en una tabla de margen proyectado
    - El margen se calcula sobre ingresos totales
    """
    df = forecast_ingresos.copy().rename(columns={"forecast_usd": "ingresos_forecast"})
    df = df.merge(
        forecast_gastos.rename(columns={"forecast_usd": "gastos_forecast"}),
        on="periodo",
        how="left",
    )
    df = df.merge(
        forecast_mrr.rename(columns={"forecast_usd": "mrr_forecast"}),
        on="periodo",
        how="left",
    )
    df["margen_usd"] = (df["ingresos_forecast"] - df["gastos_forecast"]).round(2)
    df["margen_pct"] = (
        (df["margen_usd"] / df["ingresos_forecast"] * 100)
        .where(df["ingresos_forecast"] != 0, 0)
        .round(1)
    )
    return df


def calcular_tendencia(serie: pd.Series) -> dict:
    """Calcula estadísticas de tendencia para incluir en el resumen"""
    if len(serie) < 2:
        return {}
    cambio_abs = serie.iloc[-1] - serie.iloc[0]
    cambio_pct = (cambio_abs / serie.iloc[0] * 100) if serie.iloc[0] != 0 else 0
    variacion_mensual = serie.pct_change().dropna()
    return {
        "media_mensual":    round(serie.mean(), 2),
        "max_mensual":      round(serie.max(), 2),
        "min_mensual":      round(serie.min(), 2),
        "cambio_total_pct": round(cambio_pct, 1),
        "volatilidad_pct":  round(variacion_mensual.std() * 100, 1),
        "n_periodos":       len(serie),
    }


def construir_resumen(
    df_gastos: pd.DataFrame,
    df_ingresos: pd.DataFrame,
    df_mrr: pd.DataFrame,
    resumen_cat: pd.DataFrame,
    resumen_mrr: pd.DataFrame,
    tendencia_gastos: dict,
    tendencia_ingresos: dict,
    tendencia_mrr: dict,
    margen_proyectado: pd.DataFrame,
) -> str:
    """Construye el texto de contexto que se enviará a Claude"""
    lineas = [
        "═══ ANÁLISIS FINANCIERO – INNOVA FINANCE ═══",
        f"Fecha del análisis: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"Registros de gasto   : {len(df_gastos)}",
        f"Registros de ingresos: {len(df_ingresos)}",
        f"Registros de MRR     : {len(df_mrr)}",
        "",
        "── CLASIFICACIÓN DE GASTOS POR CATEGORÍA ──",
    ]

    for _, row in resumen_cat.iterrows():
        lineas.append(
            f"  • {row['category']:<22} "
            f"USD {row['total']:>12,.2f}  ({row['pct']}%)  "
            f"[{int(row['transacciones'])} transacciones]"
        )

    lineas += ["", "── MRR ACTIVO POR PLAN ──"]
    for _, row in resumen_mrr.iterrows():
        lineas.append(
            f"  • {row['plan']:<22} "
            f"USD {row['mrr_total']:>12,.2f}  ({row['pct']}%)  "
            f"[{int(row['suscripciones'])} suscripciones]"
        )

    def _bloque_tendencia(label: str, t: dict) -> list[str]:
        if not t:
            return []
        return [
            "",
            f"── TENDENCIA HISTÓRICA – {label} ──",
            f"  Períodos analizados      : {t.get('n_periodos')} meses",
            f"  Media mensual            : USD {t.get('media_mensual'):,.2f}",
            f"  Máximo mensual           : USD {t.get('max_mensual'):,.2f}",
            f"  Mínimo mensual           : USD {t.get('min_mensual'):,.2f}",
            f"  Cambio total (inicio→fin): {t.get('cambio_total_pct')}%",
            f"  Volatilidad mensual      : {t.get('volatilidad_pct')}%",
        ]

    lineas += _bloque_tendencia("GASTOS", tendencia_gastos)
    lineas += _bloque_tendencia("INGRESOS", tendencia_ingresos)
    lineas += _bloque_tendencia("MRR", tendencia_mrr)

    if not margen_proyectado.empty:
        lineas += ["", "── FORECAST PRÓXIMOS 3 MESES ──"]
        lineas.append(
            f"  {'Período':<10}  {'Ingresos':>14}  {'MRR':>14}  "
            f"{'Gastos':>14}  {'Margen USD':>14}  {'Margen %':>9}"
        )
        lineas.append("  " + "─" * 80)
        for _, row in margen_proyectado.iterrows():
            lineas.append(
                f"  {row['periodo'].strftime('%Y-%m'):<10}  "
                f"USD {row['ingresos_forecast']:>10,.2f}  "
                f"USD {row['mrr_forecast']:>10,.2f}  "
                f"USD {row['gastos_forecast']:>10,.2f}  "
                f"USD {row['margen_usd']:>10,.2f}  "
                f"{row['margen_pct']:>8.1f}%"
            )

    return "\n".join(lineas)


def analizar_con_ia(resumen: str) -> str:
    """Envía el resumen financiero a Claude y retorna conclusiones clave"""
    if not ANTHROPIC_API_KEY:
        return (
            "[IA desactivada] Configura ANTHROPIC_API_KEY en tu archivo .env "
            "para habilitar el análisis con IA."
        )

    prompt = (
        "Eres un CFO experto en finanzas corporativas. "
        "Analiza el siguiente reporte financiero de INNOVA FINANCE y proporciona:\n\n"
        "1. **Conclusiones clave** (máx. 5 puntos): los hallazgos más importantes.\n"
        "2. **Categorías críticas**: qué áreas de gasto merecen atención urgente y por qué.\n"
        "3. **Interpretación del forecast**: ¿la tendencia es sostenible? ¿hay riesgo de sobrecosto?\n"
        "4. **Recomendaciones accionables** (máx. 3): acciones concretas para optimizar el gasto.\n"
        "5. **Alertas o riesgos**: señales de alerta que el equipo financiero debe monitorear.\n\n"
        "Sé directo, usa cifras del reporte y evita generalidades.\n\n"
        f"REPORTE:\n{resumen}"
    )

    try:
        cliente = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        respuesta = cliente.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        return respuesta.content[0].text
    except Exception as e: # pylint: disable=broad-exception-caught
        logger.error("Error al llamar a la API de Anthropic: %s", e)
        return f"[Error IA] {e}"


def ejecutar_analisis_ia() -> None:
    """Orquesta clasificación, forecast e interpretación con IA"""
    # 1 → Datos
    df_gastos   = _obtener_gastos()
    df_ingresos = _obtener_ingresos()
    df_mrr      = _obtener_mrr()

    # 2 → Resúmenes categóricos
    resumen_cat = resumen_por_categoria(df_gastos)
    resumen_mrr = resumen_mrr_por_plan(df_mrr)

    # 3 → Series temporales (cada vista tiene su columna de monto)
    serie_gastos   = _serie_mensual(df_gastos,   col_monto="gastos_usd")
    serie_ingresos = _serie_mensual(df_ingresos, col_monto="ingresos_usd")
    serie_mrr      = _serie_mensual(df_mrr,      col_monto="mrr_usd")

    # 4 → Tendencias
    tendencia_gastos   = calcular_tendencia(serie_gastos)
    tendencia_ingresos = calcular_tendencia(serie_ingresos)
    tendencia_mrr      = calcular_tendencia(serie_mrr)

    # 5 → Forecasts individuales
    forecast_gastos   = forecast_lineal(serie_gastos,   meses_adelante=3)
    forecast_ingresos = forecast_lineal(serie_ingresos, meses_adelante=3)
    forecast_mrr      = forecast_lineal(serie_mrr,      meses_adelante=3)

    # 6 → Margen proyectado consolidado
    margen_proyectado = calcular_margen_proyectado(
        forecast_ingresos, forecast_gastos, forecast_mrr
    )

    # 7 → Resumen textual
    resumen = construir_resumen(
        df_gastos, df_ingresos, df_mrr,
        resumen_cat, resumen_mrr,
        tendencia_gastos, tendencia_ingresos, tendencia_mrr,
        margen_proyectado,
    )
    logger.info("Resumen generado:\n%s", resumen)

    # 8 → IA
    interpretacion = analizar_con_ia(resumen)
    logger.info("\n==== CONCLUSIONES IA ====\n%s\n", interpretacion)

    # 9 → Guardar
    log_dir = Path(os.getenv("LOG_DIR", "logs"))
    log_dir.mkdir(exist_ok=True)
    output_path = log_dir / "analisis_financiero_ia.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(resumen)
        f.write("\n\n==== CONCLUSIONES IA ====\n")
        f.write(interpretacion)

    logger.info("Análisis completado. Resultado guardado en '%s'", output_path)


if __name__ == "__main__":
    ejecutar_analisis_ia()
