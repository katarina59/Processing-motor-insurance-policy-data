import json
from pyspark.sql.functions import (size, array_union,when, array, struct, col, lit, 
                                   length, regexp_extract, col, lit, current_timestamp, 
                                   date_format, length, regexp_extract)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Metadata-driven data processing engine for PySpark
# Dynamically executes dataflows based on JSON metadata configuration
class DataflowEngine:
    
    def __init__(self, spark, metadata_path):
        self.spark = spark
        self.metadata_path = metadata_path
        self.dataframes = {}
        
        # Load metadata
        logger.info(f"Loading metadata from: {metadata_path}")
        with open(metadata_path, 'r') as f:
            self.metadata = json.load(f)
        logger.info("Metadata loaded successfully")
    
    def execute(self):
        logger.info("Starting dataflow execution...")
        
        for dataflow in self.metadata['dataflows']:
            dataflow_name = dataflow['name']
            logger.info(f"\n{'='*60}")
            logger.info(f"Executing dataflow: {dataflow_name}")
            logger.info(f"{'='*60}\n")
            
            # Step 1: Load sources
            self._load_sources(dataflow['sources'])
            
            # Step 2: Apply transformations
            self._apply_transformations(dataflow['transformations'])
            
            # Step 3: Write to sinks
            self._write_sinks(dataflow['sinks'])
            
            logger.info(f"\nDataflow '{dataflow_name}' completed successfully\n")
    
    def _load_sources(self, sources):
        logger.info("--- Loading Sources ---")
        
        for source in sources:
            name = source['name']
            path = source['path']
            format_type = source['format'].lower()
            
            logger.info(f"Loading source '{name}' from {path} (format: {format_type})")
            
            df = self.spark.read.format(format_type).load(path)
            record_count = df.count()
            
            self.dataframes[name] = df
            logger.info(f" Source '{name}' loaded: {record_count} records")
            
            logger.info(f"Sample data from '{name}':")
            df.show(truncate=False)
    
    # Apply all transformations in sequence
    def _apply_transformations(self, transformations):
        logger.info("\n--- Applying Transformations ---")
        
        for transformation in transformations:
            trans_type = transformation['type']
            trans_name = transformation['name']
            
            logger.info(f"\nTransformation: {trans_name} (type: {trans_type})")
            
            if trans_type == 'validate_fields':
                self._validate_fields(transformation)
            elif trans_type == 'add_fields':
                self._add_fields(transformation)
            else:
                logger.warning(f"Unknown transformation type: {trans_type}")
    
    # Validate fields and split data into valid (OK) and invalid (KO) datasets
    def _validate_fields(self, transformation):
        
        params = transformation['params']
        input_name = params['input']
        df = self.dataframes[input_name]
        
        validations = params['validations']
        
        logger.info(f"Input: {input_name}")
        logger.info(f"Validations to apply: {len(validations)}")
        
        # Create a column to track all validation errors
        df_with_errors = df.withColumn('validation_errors', array())
        
        # Track overall validity
        is_valid_expr = lit(True)
        
        for validation in validations:
            field = validation['field']
            rules = validation['validations']
            
            logger.info(f"  Validating field '{field}' with rules: {rules}")
            
            for rule in rules:
                # Parse rule (can be "rule" or "rule:param")
                rule_parts = rule.split(':')
                rule_name = rule_parts[0]
                
                invalid_condition = None
                error_message = ""
                
                # RULE 1: notNull
                if rule_name == 'notNull':
                    invalid_condition = col(field).isNull()
                    error_message = f"{field} is null"
                
                # RULE 2: notEmpty
                elif rule_name == 'notEmpty':
                    invalid_condition = (col(field) == '') | (col(field).isNull())
                    error_message = f"{field} is empty"
                
                else:
                    logger.warning(f"Unknown validation rule: {rule}")
                    continue
                
                # Add error to array if this rule fails
                if invalid_condition is not None:
                    error_struct = struct(
                        lit(field).alias('field'),
                        lit(rule).alias('rule'),
                        lit(error_message).alias('message')
                    )
                    
                    df_with_errors = df_with_errors.withColumn(
                        'validation_errors',
                        when(invalid_condition, 
                            array_union(col('validation_errors'), array(error_struct))
                        ).otherwise(col('validation_errors'))
                    )
                    
                    # Track if ANY validation fails
                    is_valid_expr = is_valid_expr & ~invalid_condition
        
        # Valid records have no errors
        valid_df = df_with_errors.filter(size(col('validation_errors')) == 0).drop('validation_errors')
        
        # Invalid records have at least one error
        invalid_df = df_with_errors.filter(size(col('validation_errors')) > 0)
        
        # Store results
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        
        self.dataframes['validation_ok'] = valid_df
        logger.info(f"\n Valid records: {valid_count}")
        
        if valid_count > 0:
            logger.info("Sample valid records:")
            valid_df.show(truncate=False)
        
        if invalid_count > 0:
            self.dataframes['validation_ko'] = invalid_df
            logger.info(f"\n Invalid records: {invalid_count}")
            logger.info("Sample invalid records:")
            invalid_df.show(truncate=False)
        else:
            logger.info("\n No invalid records found")
    
    def _add_fields(self, transformation):
        params = transformation['params']
        input_name = params['input']
        
        # Check if input exists
        if input_name not in self.dataframes:
            logger.warning(f"Input '{input_name}' not found, skipping add_fields")
            return
        
        df = self.dataframes[input_name]
        
        logger.info(f"Input: {input_name}")
        logger.info(f"Adding {len(params['addFields'])} field(s)")
        
        for field in params['addFields']:
            field_name = field['name']
            function = field['function']
            
            logger.info(f"  Adding field '{field_name}' using function '{function}'")
            
            if function == 'current_timestamp':
                df = df.withColumn(field_name, date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss'))
            else:
                logger.warning(f"Unknown function: {function}")
        
        # Store with transformation name
        trans_name = transformation['name']
        self.dataframes[trans_name] = df
        
        logger.info(f" Fields added to '{trans_name}'")
        logger.info("Sample with new fields:")
        df.show(truncate=False)
    
    # Write datasets to configured sinks
    def _write_sinks(self, sinks):
        logger.info("\n--- Writing to Sinks ---")
        
        for sink in sinks:
            input_name = sink['input']
            sink_name = sink['name']
            
            logger.info(f"\nSink: {sink_name}")
            logger.info(f"Input: {input_name}")
            
            # Check if input exists
            if input_name not in self.dataframes:
                logger.warning(f"No data for input '{input_name}', skipping sink '{sink_name}'")
                continue
            
            df = self.dataframes[input_name]
            path = sink['paths'][0]
            format_type = sink['format'].lower()
            save_mode = sink['saveMode'].lower()
            
            record_count = df.count()
            
            logger.info(f"Writing {record_count} records to {path}")
            logger.info(f"Format: {format_type}, Mode: {save_mode}")
            
            # Write data
            df.coalesce(1).write.mode(save_mode).format(format_type).save(path)
            
            logger.info(f" Successfully written to {path}")