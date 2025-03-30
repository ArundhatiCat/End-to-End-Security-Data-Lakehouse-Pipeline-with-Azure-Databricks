from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='trigger_all_security_ingests',
    default_args=default_args,
    description='A DAG that triggers 4 security ingest DAGs',
    schedule_interval=None,  # Set to None for manual trigger
    start_date=days_ago(1),
    catchup=False,
) as dag:

    trigger_ingest_almalinux_advisories = TriggerDagRunOperator(
        task_id='trigger_ingest_almalinux_advisories',
        trigger_dag_id='ingest_almalinux_advisories'
    )

    trigger_check_and_ingest_vulndb = TriggerDagRunOperator(
        task_id='trigger_check_and_ingest_vulndb',
        trigger_dag_id='check_and_ingest_vulndb'
    )

    trigger_ingest_ubuntu_security_notices = TriggerDagRunOperator(
        task_id='trigger_ingest_ubuntu_security_notices',
        trigger_dag_id='ingest_ubuntu_security_notices'
    )

    trigger_ingest_bitnami_vulndb_artifactory = TriggerDagRunOperator(
        task_id='trigger_ingest_bitnami_vulndb_artifactory',
        trigger_dag_id='ingest_bitnami_vulndb_artifactory'
    )

    # If you want all the DAGs to be triggered in parallel, no dependencies are set.
    # To trigger them sequentially, you could set dependencies like:
    # trigger_ingest_almalinux_advisories >> trigger_check_and_ingest_vulndb >> trigger_ingest_ubuntu_security_notices >> trigger_ingest_bitnami_vulndb_artifactory
