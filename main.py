"""
- Punto de entrada principal del proyecto
- Ejecuta el pipeline completo: Extracción → dbt → Análisis IA
"""

from pathlib import Path
import argparse
import logging
import subprocess
from dotenv import load_dotenv

from src.pipeline_extraccion import ejecutar_pipeline
# from src.deteccion_anomalias import ejecutar_analisis_ia




load_dotenv()

log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level="INFO",
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_dir / "pipeline.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")




def ejecutar_extraccion():
    """Ejecución de la etapa de extracción"""
    logger.info("=" * 60)
    logger.info("PASO 1: PIPELINE DE EXTRACCIÓN")
    logger.info("=" * 60)
    ejecutar_pipeline()


def ejecutar_transformacion():
    """Ejecución de la etapa de extracción con dbt"""
    def ejecutar_comando_dbt(comando):
        """Ejecuta un comando de dbt y valida su resultado"""
        logger.info("Ejecutando: %s", ' '.join(comando))
        result = subprocess.run(
            comando,
            cwd="dbt_env",
            capture_output=False,
            check=False
        )
        if result.returncode != 0:
            logger.error("Falló el comando: %s", ' '.join(comando))
            return False
        return True

    logger.info("=" * 60)
    logger.info("PASO 2: TRANSFORMACIÓN CON DBT")
    logger.info("=" * 60)
    comandos = [
        ["dbt", "clean"],
        ["dbt", "debug"],
        ["dbt", "deps"],
        ["dbt", "build"],
    ]
    for comando in comandos:
        ok = ejecutar_comando_dbt(comando)
        if not ok:
            logger.error("Se detuvo el flujo de dbt por error previo.")
            return
    logger.info("Ejecución completa de dbt finalizada exitosamente.")


# def ejecutar_analisis_financiero():
#     """Integración con IA para la detección de anomalías"""
#     logger.info("=" * 60)
#     logger.info("PASO 3: DETECCIÓN DE ANOMALÍAS CON IA")
#     logger.info("=" * 60)
#     ejecutar_analisis_ia()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Financial Data Pipeline")
    parser.add_argument(
        "--step",
        choices=["extract", "transform", "ia-analysis", "all"],
        default="all",
        help="Execution step (default: all)",
    )
    args = parser.parse_args()

    if args.step in ("extract", "all"):
        ejecutar_extraccion()
    if args.step in ("transform", "all"):
        ejecutar_transformacion()
    # if args.step in ("ia-analysis", "all"):
    #     ejecutar_analisis_financiero()

    logger.info("Pipeline finalizado.")
