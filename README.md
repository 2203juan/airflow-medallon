# Airflow Medallion

Orquestación con Apache Airflow 3.3.2 del pipeline Medallion de mi laboratorio
[Dry Bean](https://github.com/2203juan/dry-bean-classifier): cada capa
(bronze → silver → gold → modelo → data warehouse) es una tarea del DAG
`medallon_etl_drybean`, con logs, reintentos e historial de ejecuciones.

Basado en el tutorial de Juan Manuel Núñez Velasco en Medium. El tutorial está escrito
para Windows con WSL2; este proyecto corre en macOS con Docker Desktop.

## Cómo funciona

```
laboratorio_drybean_ml/  (solo lectura)       airflow-medallon/data/
  src/extract, transform, load, model   ──►   bronze/ silver/ gold/
  config/config.yaml                          models/ reports/ warehouse/
            │
            ▼
  DAG medallon_etl_drybean
  validar_bronze → transformar_a_silver → integrar_a_gold → entrenar_modelo → cargar_datawarehouse
```

El DAG no reimplementa el ETL: importa las funciones del laboratorio (`clean_data`,
`variety_summary`, `split_train_test`, `train_models`…) y lee los parámetros de su
`config.yaml`. El laboratorio se monta en `/opt/airflow/medallon` en **solo lectura**,
así que Airflow no puede modificarlo; todas las salidas quedan en `data/` de este repo.

| Tarea | Qué hace | Salida |
|---|---|---|
| `validar_bronze` | Toma el CSV bronze del laboratorio (si no existe, lo descarga de UCI) y verifica que tenga filas y la columna `Class` | `data/bronze/dry_bean_raw.csv` |
| `transformar_a_silver` | Quita nulos y duplicados | `data/silver/dry_bean_clean.csv` (13.543 filas) |
| `integrar_a_gold` | Resumen por variedad y split train/test estratificado | `data/gold/*.csv` |
| `entrenar_modelo` | Entrena y compara Logistic Regression y Random Forest | `data/models/`, `data/reports/` |
| `cargar_datawarehouse` | Carga silver, gold y métricas en SQLite | `data/warehouse/drybean.db` (5 tablas) |

Cada tarea tiene 1 reintento a los 30 s. Si una falla, las siguientes no se ejecutan.

## Requisitos

- Docker Desktop abierto (Airflow pide mínimo 4 GB de RAM para Docker).
- El laboratorio Dry Bean en `../laboratorio1/laboratorio_drybean_ml`:

```bash
git clone https://github.com/2203juan/dry-bean-classifier.git ../laboratorio1/laboratorio_drybean_ml
```

Si lo tienes en otra ruta: `make up ETL_PROJECT_DIR=/ruta/al/laboratorio`.

## Uso

```bash
make all      # primera vez: construye la imagen, levanta Airflow, corre el DAG y abre el navegador
make up       # levanta Airflow
make run      # dispara el DAG y espera a que termine, mostrando el estado de cada tarea
make status   # estado de los contenedores
make down     # apaga Airflow
make help     # todos los comandos
```

Interfaz web: http://localhost:8081, usuario `airflow`, contraseña `airflow`.

`make run` muestra algo así:

```
→ Ejecutando medallon_etl_drybean  (run: manual__2026-11-21T19:54:17...)
  Estado final: success
  task_id              | state
  validar_bronze       | success
  transformar_a_silver | success
  integrar_a_gold      | success
  entrenar_modelo      | success
  cargar_datawarehouse | success
```

Otros comandos útiles: `make runs` (historial), `make errors` (errores de importación de
DAGs), `make test`, `make logs`, `make shell`.

## Resultados

Silver y los tres CSV de gold son idénticos byte a byte a los que genera `python main.py`
en el laboratorio. Métricas sobre test:

| Modelo | Accuracy | F1 macro |
|---|---|---|
| Logistic Regression | 0.9195 | 0.9306 |
| Random Forest | 0.9195 | 0.9310 |

La regresión logística da exactamente lo mismo que en el laboratorio. El Random Forest
no: con los mismos datos gold y scikit-learn 1.9.1 en ambos lados, obtiene F1 0.9296 en
macOS y 0.9310 dentro del contenedor Linux de Airflow.

## Tests

```bash
make test
```

Comprueban que el DAG carga sin errores, que no tiene programación automática
(`schedule=None`) y que las cinco tareas están encadenadas en el orden correcto.
El CI de GitHub Actions valida el `docker-compose.yaml`, construye la imagen y corre
los mismos tests.

## Cambios respecto al tutorial

- **macOS en vez de Windows + WSL2.** Docker Desktop ya trae la VM Linux, así que los
  pasos 3 a 7 (instalar y configurar WSL2) no aplican.
- **El ETL orquestado es el laboratorio Dry Bean.** El tutorial usa un proyecto de
  estudiantes cuyo código y datos no publica.
- **Montaje en solo lectura** (`:ro`) y salidas en `data/` de este repo, para no tocar
  los archivos del laboratorio.
- **Quinta tarea `entrenar_modelo`**, porque el laboratorio termina con un modelo.
  `cargar_datawarehouse` carga en SQLite, que no necesita otro servicio.
- **Puerto 8081** en vez de 8080, que en mi equipo ya estaba ocupado. Se cambia con
  `AIRFLOW_PORT` en `.env`.
- **`AIRFLOW_UID` con `id -u`** en vez de 50000: en macOS y Linux los archivos de `logs/`
  y `data/` quedan con tu usuario.
- **`LOAD_EXAMPLES` en `false`**, para que la interfaz muestre solo este DAG.
- **Los tests usan `airflow.dag_processing.dagbag.DagBag`**: en Airflow 3.3 `DagBag` ya no
  acepta `include_examples`.
