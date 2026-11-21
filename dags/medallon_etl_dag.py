"""Pipeline Medallion del laboratorio Dry Bean orquestado con Airflow.

Airflow no reimplementa el ETL: importa las funciones del laboratorio
(montado en /opt/airflow/medallon en solo lectura) y ejecuta cada capa
como una tarea independiente. Las salidas se escriben en /opt/airflow/data.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, task

PROJECT_DIR = Path("/opt/airflow/medallon")
DATA_DIR = Path("/opt/airflow/data")
sys.path.insert(0, str(PROJECT_DIR))

BRONZE = DATA_DIR / "bronze" / "dry_bean_raw.csv"
SILVER = DATA_DIR / "silver" / "dry_bean_clean.csv"
GOLD_SUMMARY = DATA_DIR / "gold" / "resumen_variedades.csv"
GOLD_TRAIN = DATA_DIR / "gold" / "train.csv"
GOLD_TEST = DATA_DIR / "gold" / "test.csv"
MODEL = DATA_DIR / "models" / "random_forest_drybean.joblib"
METRICS = DATA_DIR / "reports" / "comparacion_modelos.csv"
WAREHOUSE = DATA_DIR / "warehouse" / "drybean.db"

log = logging.getLogger(__name__)


def load_config() -> dict:
    import yaml

    return yaml.safe_load((PROJECT_DIR / "config" / "config.yaml").read_text())


with DAG(
    dag_id="medallon_etl_drybean",
    description="Pipeline ETL Medallion Dry Bean: Bronze -> Silver -> Gold -> Modelo -> Data Warehouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["etl", "medallion", "drybean"],
) as dag:

    @task
    def validar_bronze() -> dict:
        """Toma el bronze del laboratorio (o lo descarga de UCI) y valida que sirva."""
        import pandas as pd
        from src.load.load_files import save_csv
        from src.transform.clean_data import quality_report

        config = load_config()
        origen = PROJECT_DIR / config["paths"]["bronze"]
        if origen.exists():
            log.info("Usando bronze del laboratorio: %s", origen)
            df = pd.read_csv(origen)
        else:
            from src.extract.extract_uci import extract_dry_bean

            log.info("No hay bronze local; descargando dataset UCI id=%s", config["dataset"]["uci_id"])
            df = extract_dry_bean(config["dataset"]["uci_id"])

        target = config["dataset"]["target"]
        if df.empty:
            raise ValueError("Bronze está vacío")
        if target not in df.columns:
            raise ValueError(f"Bronze no tiene la columna objetivo '{target}'")

        save_csv(df, BRONZE)
        reporte = quality_report(df)
        log.info("Bronze validado: %s", reporte)
        return reporte

    @task
    def transformar_a_silver() -> int:
        """Limpia nulos y duplicados."""
        import pandas as pd
        from src.load.load_files import save_csv
        from src.transform.clean_data import clean_data

        df = clean_data(pd.read_csv(BRONZE))
        save_csv(df, SILVER)
        log.info("Silver generado: %s filas", len(df))
        return len(df)

    @task
    def integrar_a_gold() -> dict:
        """Tabla de negocio por variedad + train/test estratificado."""
        import pandas as pd
        from src.load.load_files import save_csv
        from src.transform.gold_transformations import split_train_test, variety_summary

        config = load_config()
        target = config["dataset"]["target"]
        df = pd.read_csv(SILVER)

        save_csv(variety_summary(df, target), GOLD_SUMMARY)
        train_df, test_df = split_train_test(
            df, target, config["split"]["test_size"], config["split"]["random_state"]
        )
        save_csv(train_df, GOLD_TRAIN)
        save_csv(test_df, GOLD_TEST)
        log.info("Gold generado: %s train / %s test", len(train_df), len(test_df))
        return {"train": len(train_df), "test": len(test_df)}

    @task
    def entrenar_modelo() -> list[dict]:
        """Entrena y compara los modelos del laboratorio; guarda el Random Forest."""
        import pandas as pd
        from src.load.load_files import save_csv, save_model
        from src.model.train_model import evaluate_models, train_models

        config = load_config()
        target = config["dataset"]["target"]
        train_df, test_df = pd.read_csv(GOLD_TRAIN), pd.read_csv(GOLD_TEST)
        X_train, y_train = train_df.drop(columns=target), train_df[target]
        X_test, y_test = test_df.drop(columns=target), test_df[target]

        models = train_models(X_train, y_train, config["split"]["random_state"])
        metrics = evaluate_models(models, X_test, y_test)
        save_csv(metrics, METRICS)
        save_model(models["Random Forest"], MODEL)
        log.info("Comparación de modelos:\n%s", metrics.to_string(index=False))
        return metrics.round(4).to_dict(orient="records")

    @task
    def cargar_datawarehouse() -> dict:
        """Carga silver, gold y métricas en un data warehouse SQLite."""
        import sqlite3

        import pandas as pd

        tablas = {
            "silver_dry_bean": SILVER,
            "gold_resumen_variedades": GOLD_SUMMARY,
            "gold_train": GOLD_TRAIN,
            "gold_test": GOLD_TEST,
            "metricas_modelos": METRICS,
        }
        WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
        filas = {}
        with sqlite3.connect(WAREHOUSE) as conn:
            for nombre, ruta in tablas.items():
                df = pd.read_csv(ruta)
                df.to_sql(nombre, conn, if_exists="replace", index=False)
                filas[nombre] = len(df)
        log.info("Data warehouse %s cargado: %s", WAREHOUSE, filas)
        return filas

    bronze = validar_bronze()
    silver = transformar_a_silver()
    gold = integrar_a_gold()
    modelo = entrenar_modelo()
    carga = cargar_datawarehouse()

    bronze >> silver >> gold >> modelo >> carga
