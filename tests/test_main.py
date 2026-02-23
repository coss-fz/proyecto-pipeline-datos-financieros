"""
Tests para main.py
Cubre: ejecutar_transformacion y main
"""

import sys
import logging
from unittest.mock import MagicMock, patch
import pytest

from main import ejecutar_transformacion, main # pylint: disable=wrong-import-position







class TestEjecutarTransformacion:
    """Clase para definir todos los tests de la función 'ejecutar_transformacion"""

    @patch("main.subprocess.run")
    def test_ejecuta_todos_los_comandos_dbt(self, mock_run, tmp_path, monkeypatch):
        """Debe ejecutar dbt clean, debug, deps y build en orden"""
        dbt_env = tmp_path / "dbt_env"
        dbt_env.mkdir()
        monkeypatch.chdir(tmp_path)

        mock_run.return_value = MagicMock(returncode=0)

        ejecutar_transformacion()

        comandos_ejecutados = [c.args[0] for c in mock_run.call_args_list]
        assert ["dbt", "clean"] in comandos_ejecutados
        assert ["dbt", "debug"] in comandos_ejecutados
        assert ["dbt", "deps"] in comandos_ejecutados
        assert ["dbt", "build"] in comandos_ejecutados


    @patch("main.subprocess.run")
    def test_detiene_flujo_si_dbt_falla(self, mock_run, tmp_path, monkeypatch, caplog):
        """Si dbt debug falla, no deben ejecutarse los comandos posteriores"""
        dbt_env = tmp_path / "dbt_env"
        dbt_env.mkdir()
        monkeypatch.chdir(tmp_path)

        mock_run.side_effect = [
            MagicMock(returncode=0),   # dbt clean
            MagicMock(returncode=1),   # dbt debug → falla
        ]

        with caplog.at_level(logging.ERROR, logger="extraccion"):
            ejecutar_transformacion()
        assert mock_run.call_count == 2


    def test_lanza_error_si_dbt_env_no_existe(self, tmp_path, monkeypatch):
        """Debe lanzar FileNotFoundError si la carpeta dbt_env no existe"""
        monkeypatch.chdir(tmp_path)
        with pytest.raises(FileNotFoundError, match="dbt"):
            ejecutar_transformacion()




class TestMain:
    """Clase para definir todos los tests de la función 'main"""

    @patch("main.ejecutar_transformacion")
    @patch("main.ejecutar_extraccion")
    # @patch("main.ejecutar_analisis_financiero")
    def test_step_all_ejecuta_ambas_etapas(self, mock_ext, mock_trans): # mock_af
        """--step all debe llamar a extracción y luego a transformación."""
        with patch.object(sys, "argv", ["main.py", "--step", "all"]):
            main()
        mock_ext.assert_called_once()
        mock_trans.assert_called_once()
        # mock_af.assert_called_once()


    @patch("main.ejecutar_transformacion")
    @patch("main.ejecutar_extraccion")
    # @patch("main.ejecutar_analisis_financiero")
    def test_step_especifico(self, mock_ext, mock_trans): # mock_af
        """--step extract no debe llamar a transformación ni a analisis"""
        with patch.object(sys, "argv", ["main.py", "--step", "extract"]):
            main()
        mock_ext.assert_called_once()
        mock_trans.assert_not_called()
        # mock_af.assert_not_called()


    @patch("main.ejecutar_transformacion", side_effect=RuntimeError("fallo inesperado"))
    def test_excepcion_llama_sys_exit_1(self, _):
        """Cualquier excepción dentro de 'main' debe resultar en sys.exit(1)"""
        with patch.object(sys, "argv", ["main.py", "--step", "transform"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
        assert exc_info.value.code == 1
