# ETL_modernaization_using_dynamic_DAG
ETL using dynamic DAG
1.BigQuery as a Data Lake for Raw Data : Store unaltered raw data directly from the source in BigQuery without any transformation.
2.Workflow Orchestration: Use Cloud Composer (a managed Apache Airflow service) to orchestrate the ETL/ELT pipelines, including data extraction, transformation, loading.
3.DAG Development: Develop Airflow DAGs (Directed Acyclic Graphs) to define the workflow dependencies and schedule tasks.
4.Config Driven Approach: Once the configuration file is added to GCS, a DAG will be automatically created in the Airflow UI based on the       contents of the config file.