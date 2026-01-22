from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import sys

def run_motor_pipeline():
    sys.path.insert(0, '/opt/airflow/src')
    
    from engine.dataflow_engine import DataflowEngine
    from utils.spark_session import get_spark_session
    import logging
    
    logger = logging.getLogger(__name__)
    metadata_path = "/opt/airflow/metadata/motor_insurance_config.json"
    
    try:
        logger.info("Initializing Spark session...")
        spark = get_spark_session("MotorInsurancePipeline")
        logger.info("Spark session initialized")
        
        logger.info(f"Loading metadata from: {metadata_path}")
        engine = DataflowEngine(spark, metadata_path)
        engine.execute()
        
        logger.info("Pipeline execution completed successfully")
        spark.stop()
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        raise

default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="motor_policy_ingestion",
    default_args=default_args,
    description="Metadata-driven motor policy ingestion with PySpark",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["pyspark", "metadata", "insurance"]
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    run_pipeline = PythonOperator(
        task_id="run_motor_policy_pipeline",
        python_callable=run_motor_pipeline
    )
    
    end = EmptyOperator(task_id="end")
    
    start >> run_pipeline >> end