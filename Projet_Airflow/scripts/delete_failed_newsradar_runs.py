#!/usr/bin/env python3
"""Supprime les DAG runs en etat FAILED pour newsradar_enrich_ml (metadonnees Postgres Airflow).

Usage (depuis la machine hote) :
  docker exec projet-airflow-airflow-scheduler-1 python /opt/airflow/scripts/delete_failed_newsradar_runs.py
"""
from __future__ import annotations

DAG_ID = "newsradar_enrich_ml"


def main() -> None:
    from airflow.utils.session import create_session
    from airflow.models.dagrun import DagRun
    from airflow.utils.state import DagRunState

    with create_session() as session:
        run_ids = [
            r[0]
            for r in session.query(DagRun.run_id)
            .filter(DagRun.dag_id == DAG_ID, DagRun.state == DagRunState.FAILED)
            .all()
        ]
        if not run_ids:
            print(f"Aucune execution FAILED pour {DAG_ID}.")
            return

        try:
            from airflow.models.taskinstance import TaskInstance

            session.query(TaskInstance).filter(
                TaskInstance.dag_id == DAG_ID,
                TaskInstance.run_id.in_(run_ids),
            ).delete(synchronize_session=False)
        except Exception as exc:  # noqa: BLE001
            print("Suppression TaskInstance (optionnelle):", exc)

        n = (
            session.query(DagRun)
            .filter(DagRun.dag_id == DAG_ID, DagRun.state == DagRunState.FAILED)
            .delete(synchronize_session=False)
        )
        session.commit()
        print(f"Supprime {n} execution(s) FAILED pour {DAG_ID}.")


if __name__ == "__main__":
    main()
