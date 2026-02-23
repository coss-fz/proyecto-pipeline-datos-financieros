"""
- Punto de entrada principal del proyecto
- Ejecuta el pipeline completo: Extracción → dbt → Análisis IA
"""

import sys
from pathlib import Path
import argparse
import logging
import subprocess
from dotenv import load_dotenv

from src.pipeline_extraccion import ejecutar_pipeline
from src.analisis_financiero import ejecutar_analisis_ia




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




def ejecutar_extraccion(): # pragma: no cover
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

    dbt_path = Path("dbt_env")
    if not dbt_path.exists():
        logger.error("No existe la carpeta 'dbt_env' con el proyecto dbt. No " \
                        "se puede ejecutar la transformación.")
        raise FileNotFoundError("No se encontró la carpeta de con el proyecto dbt")

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


def ejecutar_analisis_financiero(): # pragma: no cover
    """Integración con IA para el análisis financiero"""
    logger.info("=" * 60)
    logger.info("PASO 3: DETECCIÓN DE ANOMALÍAS CON IA")
    logger.info("=" * 60)
    ejecutar_analisis_ia()


def main():
    """Generar argumentos y ejecutar el pipeline completo"""
    parser = argparse.ArgumentParser(description="Financial Data Pipeline")
    parser.add_argument(
        "--step",
        choices=["extract", "transform", "ia-analysis", "all"],
        default="all",
        help="Execution step (default: all)",
    )
    args = parser.parse_args()

    try:
        if args.step in ("extract", "all"):
            ejecutar_extraccion()
        if args.step in ("transform", "all"):
            ejecutar_transformacion()
        if args.step in ("ia-analysis", "all"):
            ejecutar_analisis_financiero()
    except Exception as e: # pylint: disable=broad-exception-caught
        logger.critical("El pipeline falló inesperadamente: %s", e, exc_info=False)
        sys.exit(1)

    logger.info("Pipeline finalizado.")




if __name__ == "__main__":
    main()
