import sys
import os
from engine.dataflow_engine import DataflowEngine
from utils.spark_session import get_spark_session
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    # Get metadata path from command line or use default
    if len(sys.argv) > 1:
        metadata_path = sys.argv[1]
    else:
        metadata_path = "metadata/motor_insurance_config.json"
    
    logger.info(f"\n{'#'*70}")
    logger.info("MOTOR INSURANCE DATA PIPELINE")
    logger.info(f"{'#'*70}\n")
    logger.info(f"Metadata configuration: {metadata_path}")
    
    # Check if metadata file exists
    if not os.path.exists(metadata_path):
        logger.error(f"Metadata file not found: {metadata_path}")
        sys.exit(1)
    
    try:
        # Initialize Spark session
        logger.info("Initializing Spark session...")
        spark = get_spark_session("MotorInsurancePipeline")
        logger.info("Spark session initialized\n")
        
        # Create and execute dataflow engine
        engine = DataflowEngine(spark, metadata_path)
        engine.execute()
        
        logger.info(f"\n{'#'*70}")
        logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
        logger.info(f"{'#'*70}\n")
        
        # Stop Spark session
        spark.stop()
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()