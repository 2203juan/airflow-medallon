from pathlib import Path

from airflow.dag_processing.dagbag import DagBag

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


def get_dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR))


def test_sin_errores_de_importacion():
    assert get_dagbag().import_errors == {}


def test_dag_existe_y_es_manual():
    dag = get_dagbag().get_dag("medallon_etl_drybean")
    assert dag is not None
    assert dag.schedule is None


def test_orden_de_las_tareas():
    dag = get_dagbag().get_dag("medallon_etl_drybean")
    orden = [
        "validar_bronze",
        "transformar_a_silver",
        "integrar_a_gold",
        "entrenar_modelo",
        "cargar_datawarehouse",
    ]
    assert {t.task_id for t in dag.tasks} == set(orden)
    for anterior, siguiente in zip(orden, orden[1:]):
        assert dag.get_task(siguiente).upstream_task_ids == {anterior}
