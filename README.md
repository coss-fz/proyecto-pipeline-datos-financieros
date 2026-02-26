# proyecto-pipeline-datos-financieros
Pipeline ETL end-to-end para procesamiento de datos financieros con dbt, validación con IA y análisis empresarial avanzado.




---
## Contenidos
1. [Overview](#overview)
2. [Estructura del Proyecto](#estructura-del-proyecto)
3. [Cómo Replicar el Proyecto](#cómo-replicar-el-proyecto)
4. [Arquitectura y Diseño del Modelo](#arquitectura-y-diseño-del-modelo)
5. [Modelo de Datos (ERD)](#modelo-de-datos-erd)
6. [Pipeline: Raw → Staging → Star Schema](#pipeline-raw--staging--star-schema)
7. [Ejemplos SQL - Casos de Negocio](#ejemplos-sql---casos-de-negocio)
8. [Automatización con Airflow](#estrategia-de-orquestación-con-airflow-bonus)
9. [Escalabilidad: Múltiples Países y Monedas](#escalabilidad-múltiples-países-y-monedas)
10. [Integración IA para Forecast Financiero](#integración-ia-para-forecast-financiero)




---
## Overview
Este proyecto implementa un pipeline de ingeniería de datos empresarial diseñado para:
- **Extraer datos** desde múltiples fuentes (CSV, bases de datos)
- **Validar y limpiar** datos aplicando reglas de calidad
- **Transformar** datos crudos en un modelo dimensional (star schema) usando **dbt**
- **Analizar** datos financieros con inteligencia artificial (Claude API)
- **Generar insights** sobre MRR, CAC, FCF y otras métricas clave

### Tipo de Arquitectura
- **ELT tradicional** con validación en cada etapa
- **Modern Data Stack** usando dbt para transformaciones
- **Analytics Warehouse** con modelo dimensional (Kimball)
- **IA/ML integrado** para análisis de forecast financiero

### Stack Tecnológico
| Componente | Tecnología |
|-----------|-----------|
| **Extracción** | Python (Pandas) |
| **Orquestación** | dbt (v1.11.6) |
| **Base de Datos** | SQLite |
| **Validación** | Pandas, dbt tests |
| **IA/ML** | Claude API (Anthropic) |
| **Testing** | Pytest, pytest-cov |
| **Versionado** | Git |




---
## Estructura del Proyecto
```
proyecto-pipeline-datos-financieros/
├── data/
│   ├── raw/                                                # Datos crudos (fuente única de verdad)
│   └── innova_finance.db                                   # Base de datos procesada
├── dbt_env/                                                # Proyecto dbt
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/                                        # Capa STG: limpieza y normalización
│   │   │   ├── sources.yml
│   │   │   ├── stg_clientes.sql
│   │   │   ├── stg_empleados.sql
│   │   │   ├── stg_gastos.sql
│   │   │   ├── stg_pagos.sql
│   │   │   ├── stg_suscripciones.sql
│   │   │   └── stg_transacciones.sql
│   │   └── marts/                                          # Capa MARTS: star schema
│   │       ├── reporting/                                  # Capa REPORTING: vistas para análisis de negocio
│   │       │   ├──vw_cac_canal.sql
│   │       │   ├──vw_fcf_mensual.sql
│   │       │   ├──vw_gastos_mensuales.sql
│   │       │   ├──vw_ingresos_mensuales.sql
│   │       │   ├──vw_mrr_mensual.sql
│   │       │   └──vw_nuevos_clientes_trimestrales.sql
│   │       ├── dim_clientes.sql
│   │       ├── dim_empleados.sql
│   │       ├── dim_fecha.sql
│   │       ├── fact_gastos.sql
│   │       ├── fact_pagos.sql
│   │       ├── fact_suscripciones.sql
│   │       ├── fact_transacciones.sql
│   │       └── schema.yml
│   ├── macros/                                             # Funciones reutilizables dbt
│   ├── tests/                                              # Tests de datos
│   └── target/                                             # Salida de compilación dbt
├── src/
│   ├── pipeline_extraccion.py
│   └── analisis_financiero.py                              # Análisis IA
├── tests/                                                  # Pruebas unitarios
│   ├── test_main.py
│   └── test_pipeline_extraccion.py
├── .coveragerc
├── .env
├── .gitignore
├── main.py                                                 # Punto de entrada: orquestar todo el pipeline
├── requirements.txt
├── pytest.ini
└── README.md
```




---
## Cómo Replicar el Proyecto

### Requisitos Previos
- Python 3.12+
- Git

### Pasos de Instalación

#### 1. Clonar repositorio
```bash
git clone https://github.com/coss-fz/proyecto-pipeline-datos-financieros.git
cd proyecto-pipeline-datos-financieros
```

#### 2. Crear ambiente virtual
```bash
python -m venv .venv
source .venv/bin/activate # Windows: .venv\Scripts\activate
```

#### 3. Instalar dependencias
```bash
pip install -r requirements.txt
```

#### 4. Configurar credenciales
Crear archivo `.env` en la raíz:
```env
ANTHROPIC_API_KEY=sk-ant-...
```

#### 5. Ejecutar el pipeline completo
```bash
python main.py --step all
```
Alternativamente, por pasos:
```bash
python main.py --step extract      # Solo extracción
python main.py --step transform    # Solo transformación dbt
python main.py --step ia-analysis  # Solo análisis IA
```

#### 6. Ejecutar tests
```bash
pytest
```




---
## Arquitectura y Diseño del Modelo

### ¿Por qué este diseño? (Raw → Staging → Star Schema)

#### **Capa 1: Raw (Fuente Única de Verdad)**
**Propósito:** Mantener los datos originales intactos para auditoría y reproducibilidad.

**Características:**
- Datos con transformaciones mínimas, procurando que lleguen tal como llegan de las fuentes (CSV, APIs, DBs)
- Denominadas tablas `raw_*`
- Solo se hacen modificaciones mínimas, como deduplicación y eliminación de valores incoherentes
- Permiten "reverting" a cualquier punto en el tiempo

**Ventajas:**
- Auditoría: Siempre tenemos el dato original  
- Debugging: Podemos rastrear cambios desde la fuente  


#### **Capa 2: Staging (Preparación)**
**Propósito:** Limpiar, validar y normalizar datos crudos.

**Transformaciones llevadas a cabo:**
- **Normalización de tipos:** Texto mayúscula, fechas ISO 8601
- **Enriquecimiento:** Formato de códigos, fechas estandarizadas
- **Pseudonimización:** IDs se convierten a integers/text seguros

**¿Por qué separar?**
- Los datos staging son de vida corta (pueden regenerarse)
- Aisla lógica analítica de lógica de negocio
- Reutilizable por múltiples marts


#### **Capa 3: Marts (Star Schema - Consumo)**
**Propósito:** Exponer datos en formato optimizado para análisis (Kimball).

**Beneficios del Star Schema:**
- **Queries rápidas:** Menos JOINs, índices óptimos
- **Análisis intuitivos:** Estructura espejo de negocio
- **Desnormalización:** Datos redundantes para velocidad
- **Metricas claras:** Hechos vs Dimensiones


### ¿Por qué SIN Claves Foráneas en el Modelo Analítico?
Es una **decisión arquitectónica deliberada** en warehouse analíticos:
```sql
-- EVITAMOS ESTO
ALTER TABLE fact_transacciones
ADD FOREIGN KEY (customer_key) REFERENCES dim_clientes(customer_key);
```

#### ¿Por qué?
| Razón | Impacto |
|-------|--------|
| **Inserciones lentas** | Las FK requieren validación en cada INSERT |
| **Actualizaciones complejas** | Cambios en dimensiones son más lentos |
| **Flexibilidad en ETL** | Podemos cargar datos fuera de orden |
| **Tolerancia a cambios** | Las dimensiones pueden cambiar sin afectar facts |

#### PERO los datos SÍ se relacionan lógicamente
```sql
-- USAMOS CLAVES NATURALES
SELECT
    ft.transaction_key,
    ft.customer_key,  -- Referencia lógica a dim_clientes
    dc.country,
    dc.segment,
    ft.total_usd
FROM fact_transacciones ft
JOIN dim_clientes dc ON ft.customer_key = dc.customer_key
```
**En resumen:** Sin constraints físicos (rápido), pero con documentación clara (lógico).




---
## Modelo de Datos (ERD)
```
                                                                    DIM_FECHA
                                                            +---------------------------+
                                                            | date_day                  |
        FACT_TRANSACCIONES                                  | date_key (PK)  x----------+----------------------------------+
    +-----------------------+                               | year                      |                                  |
    | transaction_key (PK*) |                               | quarter                   |                                  |
    | date_key (FK) <-------+-----------------+             | month                     |                                  |
    | customer_key (FK) <---+-----------+     |             | day                       |                                  |
    | quantity              |           |     |             | week_of_year              |                                  |
    | unit_price_usd        |           |     |             | is_weekend                |                                  |
    | total_usd             |           |     |             +---------------------------+                                  |
    | country               |           |     |                                                                            |
    +-----------------------+           |     |                                                                            |
                                        |     +------------------------------+---------------------------------------------+
            DIM_CLIENTES                |                                    |                                             |
    +-----------------------+           |           FACT_GASTOS              |         FACT_PAGOS                          |
    | customer_key (PK) x---+-----------+         +------------------+       |       +--------------------------+          |
    | country               |           |         | date_key (FK) <--+-------+       | payment_date_key (FK) <--+----------+
    | acquisition_channel   |           |         | expense_key (PK) |               | payment_key (PK)         |          |
    | segment               |           |         | provider         |               | amount_usd               |          |
    | registration_date     |           |         | category         |               | payment_method           |          |
    +-----------------------+           |         | amount_usd       |               | transaction_key (FK*)    |          |
                                        |         | country          |               +--------------------------+          |
                                        |         +------------------+                                                     |
        FACT_SUSCRIPCIONES              |                                                                                  |
    +------------------------+          |                                                                                  |
    | subscription_key (PK)  |          |                                                                                  |
    | customer_key (FK) <----+----------+                                                                                  |
    | start_date_key (FK) <--+---------------------------------------------------------------------------------------------+
    | end_date_key (FK) <----+---------------------------------------------------------------------------------------------+
    | plan                   |
    | status                 |
    | monthly_price_usd      |
    +------------------------+

        DIM_EMPLEADOS
    +-----------------------+
    | employee_key (PK)     |
    | name                  |
    | department            |
    | salary_usd            |
    | hire_date             |
    +-----------------------+
```




---
## Pipeline: Raw → Staging → Star Schema

### Flujo de Datos Completo
```
                    ┌───────────────────────────────────────────────────────────────────┐
                    │                     FLUJO ELT COMPLETO                            │
                    └───────────────────────────────────────────────────────────────────┘

                                    EXTRACCIÓN (pipeline_extraccion.py)
                                        ↓
                                        ├─ customers.csv
                                        ├─ transactions.csv
                                        ├─ expenses.csv
                                        ├─ payments.csv
                                        ├─ subscriptions.csv
                                        └─ employees.csv
                                        ↓
                                    VALIDACIÓN INICIAL (pipeline_extraccion.py)
                                        ├─ Detección de nulos
                                        ├─ Eliminación de duplicados
                                        ├─ Normalización de fechas
                                        ├─ Validación de montos positivos
                                        └─ Guardado en SQLite (raw_*)
                                        ↓
                                    TRANSFORMACIÓN STAGING (dbt)
                                        ├─ stg_clientes.sql
                                        ├─ stg_transacciones.sql
                                        ├─ stg_gastos.sql
                                        ├─ stg_pagos.sql
                                        ├─ stg_suscripciones.sql
                                        └─ stg_empleados.sql
                                        ↓
                                    TRANSFORMACIÓN MARTS (dbt)
                                        ├─ dim_clientes.sql
                                        ├─ dim_empleados.sql
                                        ├─ dim_fecha.sql
                                        ├─ fact_transacciones.sql
                                        ├─ fact_gastos.sql
                                        ├─ fact_pagos.sql
                                        └─ fact_suscripciones.sql
                                        ↓
                                    CREACIÓN VISTAS (dbt)
                                        ├──vw_cac_canal.sql
                                        ├──vw_fcf_mensual.sql
                                        ├──vw_gastos_mensuales.sql
                                        ├──vw_ingresos_mensuales.sql
                                        ├──vw_mrr_mensual.sql
                                        └──vw_nuevos_clientes_trimestrales.sql
                                        ↓
                                    CONSUMO ANALÍTICO
                                        ├─ Dashboards BI
                                        ├─ Consultas SQL ad-hoc
                                        ├─ Análisis IA (Claude)
                                        └─ Reportes automatizados

```

### Código del Pipeline (`main.py`)
```python
# PASO 1: EXTRACCIÓN
ejecutar_extraccion()
    └─ Lee CSVs → valida → carga en SQLite raw_*

# PASO 2: TRANSFORMACIÓN CON DBT
ejecutar_transformacion()
    ├─ dbt clean   (limpia artefactos previos)
    ├─ dbt debug   (verifica conectividad)
    ├─ dbt deps    (descarga paquetes)
    └─ dbt build   (STG → MARTS, ejecuta tests)

# PASO 3: ANÁLISIS IA
ejecutar_analisis_financiero()
    └─ Genera forecast y envía resumen a Claude para análisis
```




---
## Ejemplos SQL - Casos de Negocio

### Pregunta 1: ¿Cuál fue el MRR total de agosto de 2024?
```sql
SELECT
	printf('%04d-%02d', anio, mes) as periodo,
	SUM(mrr_usd) as mrr_total_usd
FROM vw_mrr_mensual
WHERE anio = 2024 AND mes = 8;

-- periodo   | mrr_total_usd
-- 2024-08   | 66.220,00
```

### Pregunta 2: ¿Cuántos clientes nuevos se registraron durante Q1 2024?
```sql
SELECT
	trimestre,
	SUM(nuevos_clientes) AS nuevos_clientes
FROM vw_nuevos_clientes_trimestrales
WHERE anio = 2024 AND trimestre = 'Q1';

-- trimestre  | nuevos_clientes
-- Q1         | 235
```

### Pregunta 3: ¿Cuál fue el total de gastos de marketing en H1 2024?
```sql
SELECT
	category as categoria,
	SUM(gastos_usd) AS total_gastos_usd
FROM vw_gastos_mensuales
WHERE anio = 2024 AND mes <= 6 AND category = 'MARKETING';

-- categoria   | total_gastos_usd_h1
-- MARKETING   | 738.573,00
```

### Pregunta 4: ¿Cuál fue el Free Cash Flow (FCF) en diciembre de 2024?
```sql
SELECT
	printf('%04d-%02d', anio, mes) as periodo,
	ingresos_usd,
	gastos_usd,
	fcf_usd
FROM vw_fcf_mensual
WHERE anio = 2024 AND mes = 12;

-- Resultado esperado:
-- periodo  | ingresos_usd  | gastos_usd  | fcf
-- 2024-12  | 51.140,00     | 618.890,00  | -567.750,00
```

### Pregunta 5: ¿Cuál fue el país con mayor ingreso total durante 2024?
```sql
SELECT
	pais,
	anio,
	SUM(ingresos_usd) AS total_ingresos
FROM vw_ingresos_mensuales WHERE anio = 2024
GROUP BY pais
ORDER BY total_ingresos DESC
LIMIT 1;

-- Resultado esperado:
-- pais         | anio  | total_ingresos
-- COSTA RICA   | 2024  | 174.510,00
```

### Pregunta 6: ¿Cuál fue el CAC promedio anual considerando gastos de marketing y nuevos clientes?
```sql
SELECT
	anio,
	ROUND(AVG(cac_usd)) AS cac_promedio_usd
FROM vw_cac_canal
WHERE anio = 2024
GROUP BY anio;

-- Resultado esperado:
-- year | cac_promedio_usd
-- 2024 | 1.513,00
```

---
## Estrategia de Orquestación con Airflow (Bonus)
```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def notificar_finalizacion(**kwargs):
    """Envía resumen por Email/Slack"""
    ts = kwargs['ts']
    print(f"Pipeline completado en {ts}")
    # Integrar con Slack/Email aquí


default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['data-alerts@company.com'],
}

with DAG(
    'pipeline_financiero',
    default_args=default_args,
    description='Pipeline Financiero: Extracción → DBT → Análisis IA',
    schedule='0 0 * * *',
    start_date=datetime(2026,1,1),
    catchup=False,
) as dag:

    # TASK 1: Extracción de Datos
    task_extract_data = BashOperator(
        task_id='task_extract_data',
        bash_command='python main.py --step extract',
    )

    # TASK 2: Transformación con dbt
    task_transform_data = BashOperator(
        task_id='task_transform_data',
        bash_command='python main.py --step transform',
    )

    # TASK 3: Forecast + Análisis IA
    task_ia_analysis = BashOperator(
        task_id='task_ia_analysis',
        bash_command='python main.py --step ia-analysis',
    )

    # TASK 4: Notificación de resultados
    task_notify = PythonOperator(
        task_id='task_notify',
        python_callable=notificar_finalizacion,
    )

    task_extract_data >> task_transform_data >> task_ia_analysis >> task_notify
```

### Beneficios de Airflow
| Aspecto | Beneficio |
|--------|----------|
| **Scheduling** | Ejecuciones regulares (diarias, horarias) |
| **Monitoreo** | Dashboard con estado de tareas |
| **Reintentos** | Manejo automático de fallos transitorios |
| **Alertas** | Notificaciones automáticas personalizadas |
| **Auditoría** | Logs de cada ejecución |
| **Escalabilidad** | Distribuido en múltiples workers |




---
## Escalabilidad: Múltiples Países y Monedas

### Problema Actual
El modelo actual asume una única moneda, por lo que no hay cambio dinámico entre tasas.

### Solución: Dimensión de Monedas + Tasa de Cambio

#### 1. Nueva Tabla: `dim_moneda`
```sql
CREATE TABLE dim_moneda (
    currency_key INT,
    code TEXT,              -- 'USD', 'EUR', 'MXN', 'COP', etc.
    currency_name TEXT,
    country TEXT,
    usd_conv_rate REAL,     -- Tasa de cambio actual
    last_updated_at DATE
);

-- Ejemplo de Datos:
INSERT INTO dim_moneda VALUES
(1, 'USD', 'DÓLAR ESTADOUNIDENSE', 'UNITED STATES', 1.00, DATE('now')),
(2, 'MXN', 'PESO MEXICANO', 'MEXICO', 20.12, DATE('now')),
(3, 'EUR', 'EURO', 'EU', 0.9200, DATE('now')),
(4, 'COP', 'PESO COLOMBIANO', 'COLOMBIA', 850.0000, DATE('now'));
```

#### 2. Aumentar `fact_*`
```sql
ALTER TABLE fact_* ADD COLUMN (
    currency_key INT,
    total_local_currency REAL,
    applied_conv_rate REAL,
    total_usd_normalized REAL
);

-- Nueva query con normalización FX automática (ejemplo transacciones):
SELECT
    ft.transaction_key,
    ft.customer_key,
    ft.date_key,
    ft.total_local_currency,
    dm.code AS currency,
    dm.usd_conv_rate,
    ft.total_local_currency * dm.usd_conv_rate AS total_usd_equivalent,
    ft.country
FROM fact_transacciones ft
INNER JOIN dim_moneda dm ON ft.currency_key = dm.currency_key;
```

#### 3. Tabla de Tipo de Cambio Histórica
```sql
CREATE TABLE aux_tasa_cambio_historica (
    conv_rate_key INT,
    currency_key INT,
    date_key INT,
    usd_conv_rate REAL
);

-- Consultamos tasa histórica para un período específico (ejemplo transacciones):
SELECT
    ft.transaction_key,
    ft.date_key,
    ft.total_local_currency,
    tch.usd_conv_rate,
    ft.total_local_currency * tch.usd_conv_rate AS total_usd
FROM fact_transacciones ft
INNER JOIN tasa_cambio_historica tch 
    ON ft.currency_key = tch.currency_key
    AND ft.date_key = tch.date_key;
```



---
## Integración IA para Forecast Financiero
```
                ┌──────────────────────────────────────────────────────────────────────┐
                │                MÓDULO DE IA - ANÁLISIS FINANCIERO                    │
                └──────────────────────────────────────────────────────────────────────┘

                        PASO 1: EXTRACCIÓN DE DATOS
                            ├─ Leer fact_gastos, fact_pagos, fact_suscripciones
                            └─ Agrupar por mes/categoría
                            ↓
                        PASO 2: PROCESAMIENTO ESTADÍSTICO (NumPy/Statsmodels)
                            └─ Forecast: Regresión lineal simple
                            ↓
                        PASO 3: ENVÍO A CLAUDE (LLM)
                            ├─ Context: Resumen consolidado de datos
                            ├─ Prompt: "¿Cuáles son las anomalías? ¿Qué riesgos ves?"
                            └─ Response: Análisis narrativo, recomendaciones
                            ↓
                        PASO 4: REPORTE SALIDA
                            ├─ Archivo: logs/analisis_financiero_ia.txt
                            └─ Posible Dashboard: Visualización de forecasts

```
