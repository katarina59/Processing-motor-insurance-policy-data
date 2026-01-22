import json
from pyspark.sql.functions import col, lit, current_timestamp, date_format, create_map, length, regexp_extract
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
        
        # Start with all records as potentially valid
        valid_df = df
        invalid_records = []
        
        for validation in validations:
            field = validation['field']
            rules = validation['validations']
            
            logger.info(f"  Validating field '{field}' with rules: {rules}")
            
            for rule in rules:
                # Parse rule (can be "rule" or "rule:param")
                rule_parts = rule.split(':')
                rule_name = rule_parts[0]
                rule_param = rule_parts[1] if len(rule_parts) > 1 else None
                
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
                
                # RULE 3: minLength
                elif rule_name == 'minLength' and rule_param:
                    min_len = int(rule_param)
                    invalid_condition = (length(col(field)) < min_len) | (col(field).isNull())
                    error_message = f"{field} length < {min_len}"
                
                # RULE 4: plateFormat (e.g., ABC-123)
                elif rule_name == 'plateFormat':
                    # Regex for plate format: 3 letters, dash, 3 digits
                    plate_pattern = r'^[A-Z]{3}-[0-9]{3}$'
                    invalid_condition = (
                        (regexp_extract(col(field), plate_pattern, 0) == '') |
                        (col(field).isNull())
                    )
                    error_message = f"{field} invalid format (expected: XXX-999)"
                
                # RULE 5: range (e.g., range:18-80)
                elif rule_name == 'range' and rule_param:
                    min_val, max_val = map(int, rule_param.split('-'))
                    invalid_condition = (
                        (col(field) < min_val) |
                        (col(field) > max_val) |
                        (col(field).isNull())
                    )
                    error_message = f"{field} not in range [{min_val}, {max_val}]"
                
                # RULE 6: positive
                elif rule_name == 'positive':
                    invalid_condition = (col(field) <= 0) | (col(field).isNull())
                    error_message = f"{field} is not positive"
                
                # RULE 7: maxValue
                elif rule_name == 'maxValue' and rule_param:
                    max_val = float(rule_param)
                    invalid_condition = (col(field) > max_val) | (col(field).isNull())
                    error_message = f"{field} exceeds max value {max_val}"
                
                else:
                    logger.warning(f"Unknown validation rule: {rule}")
                    continue
                
                # Find invalid records for this rule
                if invalid_condition is not None:
                    invalid = valid_df.filter(invalid_condition)
                    invalid_count = invalid.count()
                    
                    if invalid_count > 0:
                        # Add validation error information
                        invalid = invalid.withColumn(
                            'validation_errors',
                            create_map(
                                lit('field'), lit(field),
                                lit('rule'), lit(rule),
                                lit('message'), lit(error_message)
                            )
                        )
                        invalid_records.append(invalid)
                        logger.info(f"     Found {invalid_count} records failing '{rule}' on {field}")
                    
                    # Keep only valid records for next validation
                    valid_df = valid_df.filter(~invalid_condition)
        
        # Store validation results
        valid_count = valid_df.count()
        self.dataframes['validation_ok'] = valid_df
        logger.info(f"\n Valid records: {valid_count}")
        
        if valid_count > 0:
            logger.info("Sample valid records:")
            valid_df.show(truncate=False)
        
        # Combine all invalid records
        if invalid_records:
            invalid_df = invalid_records[0]
            for inv in invalid_records[1:]:
                invalid_df = invalid_df.union(inv)
            
            invalid_count = invalid_df.count()
            self.dataframes['validation_ko'] = invalid_df
            logger.info(f"\n Invalid records: {invalid_count}")
            
            if invalid_count > 0:
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
            df.write.mode(save_mode).format(format_type).save(path)
            
            logger.info(f" Successfully written to {path}")