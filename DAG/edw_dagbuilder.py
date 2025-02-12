import json
from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# Declare the bucket name
bucket_name = "us-central1-test-dynamic-da-26626756-bucket"

def create_dag_from_config(config):
    dag_id = config["dag_id"]
    schedule_interval = config["schedule_interval"]
    catchup = config.get("catchup", False)
    is_paused_upon_creation = config.get("is_paused_upon_creation", False)
    sql_files = config.get("sql_files", [])

    def read_sql_from_gcs(file_path):
        """Read SQL content from a file in GCS."""
        gcs_hook = GCSHook()
        file_content = gcs_hook.download(bucket_name, file_path).decode('utf-8')
        return file_content

    def create_dynamic_dag():
        dag = DAG(
            dag_id=dag_id,
            default_args={
                'start_date': days_ago(1),
            },
            schedule_interval=schedule_interval,
            catchup=catchup,
            is_paused_upon_creation=is_paused_upon_creation,
        )

        with dag:
            previous_task = None
            for index, sql_file in enumerate(sql_files):
                sql_query = read_sql_from_gcs(sql_file)
                task = BigQueryInsertJobOperator(
                    task_id=f'run_query_{index + 1}',
                    configuration={
                        "query": {
                            "query": sql_query,
                            "useLegacySql": False,
                        }
                    },
                )
                if previous_task:
                    previous_task >> task
                previous_task = task

        return dag

    globals()[dag_id] = create_dynamic_dag()


# Path to the GCS folder
config_folder = "config/"
gcs_hook = GCSHook()

# Fetch the list of configuration files from GCS
config_files = gcs_hook.list(bucket_name, prefix=config_folder)

# Dynamically create DAGs for each config file
for config_file in config_files:
    if config_file.endswith('.json'):
        # Download the configuration file from GCS
        config_content = gcs_hook.download(bucket_name, config_file).decode('utf-8')
        config = json.loads(config_content)
        create_dag_from_config(config)