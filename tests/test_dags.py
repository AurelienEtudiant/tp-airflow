"""
Tests de base pour les DAGs — Formation Airflow IPSSI
Verifie que tous les DAGs dans dags/ se parsent sans erreur.
"""

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


def test_no_import_errors(dagbag):
    """Tous les DAGs doivent se parser sans erreur d'import."""
    assert not dagbag.import_errors, (
        f"Import errors found:\n"
        + "\n".join(f"  {k}: {v}" for k, v in dagbag.import_errors.items())
    )


def test_dags_found(dagbag):
    """Au moins un DAG doit etre present."""
    assert len(dagbag.dags) > 0, "No DAGs found in dags/ folder"


def test_no_catchup_true(dagbag):
    """Aucun DAG ne doit avoir catchup=True (anti-pattern)."""
    violations = [
        dag_id for dag_id, dag in dagbag.dags.items()
        if dag.catchup is True
    ]
    assert not violations, f"DAGs with catchup=True: {violations}"


def test_all_dags_have_tags(dagbag):
    """Tous les DAGs doivent avoir au moins un tag."""
    violations = [
        dag_id for dag_id, dag in dagbag.dags.items()
        if not dag.tags
    ]
    assert not violations, f"DAGs without tags: {violations}"
