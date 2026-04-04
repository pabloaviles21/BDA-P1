"""
Trusted Zone Pipeline - Data Quality Assessment and Cleaning

This module implements data quality checks and cleaning processes for the Trusted Zone.
It applies Denial Constraints to identify and remove invalid records across three datasets:
- uber_data
- weather_data
- accidents_data

All processing is performed using Apache Spark and SparkSQL for optimal performance.

Author: Data Engineering Team
Date: 2026-04-04
"""

import logging
import sys
from typing import Dict, List, Tuple
from datetime import datetime

import duckdb
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr

# ============================================================================
# CONFIGURATION & DENIAL CONSTRAINTS DEFINITION
# ============================================================================

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database paths
FORMATTED_DB_PATH = "formatted_zone.db"
TRUSTED_DB_PATH = "trusted_zone.db"

# ============================================================================
# DENIAL CONSTRAINTS CONFIGURATION
# ============================================================================

DENIAL_CONSTRAINTS = {
    "uber_data": {
        "table_name": "uber_data",
        "constraints": [
            {
                "name": "wave_number_positive",
                "description": "Wave number must be positive (> 0)",
                "violation_condition": "wave_number <= 0",
            },
            {
                "name": "dispatched_trips_consistency",
                "description": "Total dispatched trips must be >= unique dispatched vehicles",
                "violation_condition": "total_dispatched_trips < unique_dispatched_vehicle",
            },
            {
                "name": "date_range_validity",
                "description": "Pickup start date must be <= pickup end date",
                "violation_condition": "pickup_start_date > pickup_end_date",
            },
            {
                "name": "week_number_valid_range",
                "description": "Week number must be between 1 and 52",
                "violation_condition": "week_number < 1 OR week_number > 52",
            },
        ],
    },
    "weather_data": {
        "table_name": "weather_data",
        "constraints": [
            {
                "name": "temperature_logical_order",
                "description": "Min temperature must be <= Average <= Max temperature",
                "violation_condition": "minimum_temperature > average_temperature OR average_temperature > maximum_temperature",
            },
            {
                "name": "precipitation_non_negative",
                "description": "Precipitation must be non-negative",
                "violation_condition": "precipitation < 0",
            },
            {
                "name": "snow_fall_non_negative",
                "description": "Snow fall must be non-negative",
                "violation_condition": "snow_fall < 0",
            },
            {
                "name": "snow_depth_consistency",
                "description": "Snow depth must be non-negative and realistic",
                "violation_condition": "snow_depth < 0 OR snow_depth > 1000",
            },
        ],
    },
    "accidents_data": {
        "table_name": "accidents_data",
        "constraints": [
            {
                "name": "injury_categories_sum",
                "description": "Total injured must equal sum of injured by category",
                "violation_condition": "persons_injured != (pedestrians_injured + cyclists_injured + motorists_injured)",
            },
            {
                "name": "death_categories_sum",
                "description": "Total killed must equal sum of killed by category",
                "violation_condition": "persons_killed != (pedestrians_killed + cyclists_killed + motorists_killed)",
            },
            {
                "name": "location_validity",
                "description": "NYC coordinates: latitude between 40.5-40.9, longitude between -74.3--73.7",
                "violation_condition": "latitude < 40.5 OR latitude > 40.9 OR longitude < -74.3 OR longitude > -73.7",
            },
            {
                "name": "non_negative_injuries",
                "description": "All injury counts must be non-negative",
                "violation_condition": "persons_injured < 0 OR pedestrians_injured < 0 OR cyclists_injured < 0 OR motorists_injured < 0",
            },
        ],
    },
}


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def create_spark_session() -> SparkSession:
    """
    Create and configure a Spark session with optimized settings for data quality processing.
    
    Configuration:
    - Adaptive query execution for better performance
    - Partition coalescing for shuffle operations
    - Conservative memory settings for Windows environments
    - Broadcasting threshold optimization
    
    Returns:
        SparkSession: Configured Spark session for data processing.
    
    Raises:
        RuntimeError: If Spark session creation fails.
    """
    try:
        spark = SparkSession.builder \
            .appName("TrustedZonePipeline") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("✓ Spark session created successfully")
        return spark
    
    except Exception as e:
        logger.error(f"✗ Failed to create Spark session: {str(e)}")
        raise RuntimeError(f"Spark session creation failed: {str(e)}")


def read_from_formatted_zone(table_name: str) -> DataFrame:
    """
    Read data from the Formatted Zone (DuckDB) and convert to Spark DataFrame.
    
    This function:
    1. Connects to formatted_zone.db using DuckDB
    2. Reads the specified table using Pandas (most reliable method)
    3. Converts Pandas DataFrame to Spark DataFrame
    
    Args:
        table_name (str): Name of the table to read from formatted_zone.db.
    
    Returns:
        DataFrame: Spark DataFrame containing the data.
    
    Raises:
        ValueError: If table does not exist in formatted_zone.db.
        Exception: If DuckDB or Spark conversion fails.
    """
    try:
        # Connect to DuckDB and read table
        logger.info(f"  → Reading table '{table_name}' from {FORMATTED_DB_PATH}...")
        conn = duckdb.connect(FORMATTED_DB_PATH, read_only=True)
        
        # Verify table exists
        tables = conn.execute("SELECT table_name FROM information_schema.tables").fetchall()
        table_names = [t[0] for t in tables]
        
        if table_name not in table_names:
            raise ValueError(f"Table '{table_name}' not found in {FORMATTED_DB_PATH}. "
                           f"Available tables: {table_names}")
        
        # Read using df() method which returns a proper Pandas DataFrame
        logger.info(f"  → Reading from DuckDB...")
        pandas_df = conn.execute(f"SELECT * FROM {table_name}").df()
        record_count = len(pandas_df)
        conn.close()
        
        # Get Spark session and convert from Pandas
        logger.info(f"  → Converting to Spark DataFrame...")
        spark = SparkSession.getActiveSession()
        spark_df = spark.createDataFrame(pandas_df)
        
        logger.info(f"  ✓ Loaded {record_count:,} records from '{table_name}'")
        
        return spark_df
    
    except Exception as e:
        logger.error(f"✗ Failed to read '{table_name}' from Formatted Zone: {str(e)}")
        raise


def save_to_trusted_zone(spark_df: DataFrame, table_name: str) -> None:
    """
    Save a cleaned Spark DataFrame to the Trusted Zone (DuckDB).
    
    This function:
    1. Converts Spark DataFrame to Pandas DataFrame
    2. Connects to trusted_zone.db (creates it if needed)
    3. Writes the table (overwrites if exists)
    
    Args:
        spark_df (DataFrame): Spark DataFrame to save.
        table_name (str): Name for the table in trusted_zone.db.
    
    Raises:
        Exception: If conversion or DuckDB operations fail.
    """
    try:
        logger.info(f"  → Converting Spark DataFrame to Pandas for '{table_name}'...")
        pandas_df = spark_df.toPandas()
        
        logger.info(f"  → Writing '{table_name}' to {TRUSTED_DB_PATH}...")
        conn = duckdb.connect(TRUSTED_DB_PATH)
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM pandas_df")
        conn.close()
        
        record_count = len(pandas_df)
        logger.info(f"  ✓ Saved {record_count:,} clean records to '{table_name}' in Trusted Zone")
    
    except Exception as e:
        logger.error(f"✗ Failed to save '{table_name}' to Trusted Zone: {str(e)}")
        raise


# ============================================================================
# DATA QUALITY ASSESSMENT
# ============================================================================

def assess_data_quality(spark_df: DataFrame, dataset_key: str) -> Dict[str, int]:
    """
    Perform data quality assessment for a dataset using Denial Constraints.
    
    This function:
    1. Iterates through all Denial Constraints for the dataset
    2. Counts records that VIOLATE each constraint
    3. Prints a detailed quality report to console
    4. Returns violation counts for tracking
    
    Args:
        spark_df (DataFrame): Spark DataFrame to assess.
        dataset_key (str): Key in DENIAL_CONSTRAINTS dict (e.g., 'uber_data').
    
    Returns:
        Dict[str, int]: Dictionary mapping constraint names to violation counts.
    
    Raises:
        KeyError: If dataset_key not found in DENIAL_CONSTRAINTS.
    """
    if dataset_key not in DENIAL_CONSTRAINTS:
        raise KeyError(f"Dataset '{dataset_key}' not found in DENIAL_CONSTRAINTS configuration")
    
    constraints = DENIAL_CONSTRAINTS[dataset_key]["constraints"]
    
    # Cache the dataframe to avoid re-reading
    spark_df.cache()
    
    # Count total records once
    try:
        total_records = spark_df.count()
    except Exception as e:
        logger.error(f"✗ Error counting total records: {str(e)}")
        spark_df.unpersist()
        raise
    
    violations_report = {}
    
    logger.info(f"\n" + "=" * 80)
    logger.info(f"📊 DATA QUALITY ASSESSMENT: {dataset_key.upper()}")
    logger.info(f"=" * 80)
    logger.info(f"Total records to assess: {total_records:,}")
    logger.info(f"Total constraints: {len(constraints)}")
    logger.info(f"{'-' * 80}")
    
    for i, constraint in enumerate(constraints, 1):
        constraint_name = constraint["name"]
        violation_condition = constraint["violation_condition"]
        description = constraint["description"]
        
        try:
            # Count records that violate the constraint
            violation_count = spark_df.filter(violation_condition).count()
            violations_report[constraint_name] = violation_count
            
            # Calculate percentage
            violation_percentage = (violation_count / total_records * 100) if total_records > 0 else 0
            
            # Print formatted report
            status = "✗ VIOLATIONS FOUND" if violation_count > 0 else "✓ PASSED"
            logger.info(f"[{i}/{len(constraints)}] {status}")
            logger.info(f"   Constraint: {constraint_name}")
            logger.info(f"   Description: {description}")
            logger.info(f"   Violations: {violation_count:,} ({violation_percentage:.2f}%)")
            logger.info(f"{'-' * 80}")
        
        except Exception as e:
            logger.error(f"   ✗ Error evaluating constraint '{constraint_name}': {str(e)}")
            violations_report[constraint_name] = -1  # Indicate evaluation error
    
    # Summary statistics
    total_violations = sum([v for v in violations_report.values() if v >= 0])
    clean_records = total_records - total_violations
    quality_score = (clean_records / total_records * 100) if total_records > 0 else 0
    
    logger.info(f"📈 SUMMARY:")
    logger.info(f"   Total violations across all constraints: {total_violations:,}")
    logger.info(f"   Clean records (no violations): {clean_records:,}")
    logger.info(f"   Data quality score: {quality_score:.2f}%")
    logger.info(f"=" * 80 + "\n")
    
    # Unpersist cache
    spark_df.unpersist()
    
    return violations_report


# ============================================================================
# DATA CLEANING
# ============================================================================

def clean_dataset(spark_df: DataFrame, dataset_key: str) -> DataFrame:
    """
    Apply data cleaning using Denial Constraints (inverse logic).
    
    This function:
    1. Applies INVERSE conditions of all constraints to keep only valid records
    2. Removes duplicate records using dropDuplicates()
    3. Returns the cleaned DataFrame
    4. Logs cleaning statistics
    
    Args:
        spark_df (DataFrame): Spark DataFrame to clean.
        dataset_key (str): Key in DENIAL_CONSTRAINTS dict.
    
    Returns:
        DataFrame: Cleaned Spark DataFrame.
    
    Raises:
        KeyError: If dataset_key not found in DENIAL_CONSTRAINTS.
    """
    if dataset_key not in DENIAL_CONSTRAINTS:
        raise KeyError(f"Dataset '{dataset_key}' not found in DENIAL_CONSTRAINTS configuration")
    
    constraints = DENIAL_CONSTRAINTS[dataset_key]["constraints"]
    
    # Cache to avoid re-reading during filtering
    spark_df.cache()
    initial_count = spark_df.count()
    
    logger.info(f"\n🧹 CLEANING: {dataset_key.upper()}")
    logger.info(f"Initial record count: {initial_count:,}")
    logger.info(f"Applying {len(constraints)} constraints...")
    
    # Build the valid records filter (inverse of all violations)
    valid_filter = "1=1"  # Start with a no-op condition
    
    for constraint in constraints:
        violation_condition = constraint["violation_condition"]
        constraint_name = constraint["name"]
        
        # Inverse logic: NOT(violation_condition)
        # We need to wrap it properly with parentheses
        inverse_condition = f"NOT({violation_condition})"
        valid_filter = f"({valid_filter}) AND ({inverse_condition})"
    
    # Apply the combined filter to keep only valid records
    cleaned_df = spark_df.filter(valid_filter)
    
    # Cache before removing duplicates
    cleaned_df.cache()
    
    # Remove duplicate records
    cleaned_df = cleaned_df.dropDuplicates()
    
    final_count = cleaned_df.count()
    removed_count = initial_count - final_count
    removal_percentage = (removed_count / initial_count * 100) if initial_count > 0 else 0
    
    logger.info(f"Records after quality filtering: {final_count:,}")
    logger.info(f"Records removed (invalid): {removed_count:,} ({removal_percentage:.2f}%)")
    logger.info(f"Data quality improved: {final_count:,} valid records retained ✓")
    
    # Unpersist original cached df
    spark_df.unpersist()
    
    return cleaned_df


# ============================================================================
# MAIN PIPELINE ORCHESTRATION
# ============================================================================

def execute_trusted_zone_pipeline() -> None:
    """
    Execute the complete Trusted Zone pipeline.
    
    Pipeline stages:
    1. Initialize Spark session
    2. For each dataset (uber_data, weather_data, accidents_data):
       a. Read from Formatted Zone
       b. Assess data quality against Denial Constraints
       c. Clean data (apply constraint inverses + dedup)
       d. Save to Trusted Zone
    3. Generate final summary report
    4. Clean up resources
    
    Raises:
        Exception: If any stage of the pipeline fails.
    """
    spark = None
    
    try:
        # ====================================================================
        # STAGE 1: Initialize Spark Session
        # ====================================================================
        logger.info("\n" + "=" * 80)
        logger.info("🚀 TRUSTED ZONE PIPELINE - INITIALIZATION")
        logger.info("=" * 80)
        spark = create_spark_session()
        
        # ====================================================================
        # STAGE 2: Process Each Dataset
        # ====================================================================
        logger.info(f"\n📋 Processing {len(DENIAL_CONSTRAINTS)} datasets...\n")
        
        pipeline_summary = {}
        
        for dataset_key, dataset_config in DENIAL_CONSTRAINTS.items():
            table_name = dataset_config["table_name"]
            
            try:
                logger.info(f"\n{'#' * 80}")
                logger.info(f"# Processing: {table_name}")
                logger.info(f"{'#' * 80}")
                
                # Step 1: Read from Formatted Zone
                logger.info(f"\n[STEP 1: LOAD]")
                spark_df = read_from_formatted_zone(table_name)
                
                # Step 2: Assess data quality
                logger.info(f"\n[STEP 2: ASSESS]")
                violations = assess_data_quality(spark_df, dataset_key)
                
                # Step 3: Clean data
                logger.info(f"\n[STEP 3: CLEAN]")
                cleaned_df = clean_dataset(spark_df, dataset_key)
                
                # Step 4: Save to Trusted Zone
                logger.info(f"\n[STEP 4: SAVE]")
                save_to_trusted_zone(cleaned_df, table_name)
                
                # Record statistics
                pipeline_summary[table_name] = {
                    "initial_count": spark_df.count(),
                    "final_count": cleaned_df.count(),
                    "violations": violations,
                }
                
                logger.info(f"\n✓ Successfully processed '{table_name}'")
            
            except Exception as e:
                logger.error(f"\n✗ Error processing '{table_name}': {str(e)}")
                pipeline_summary[table_name] = {"error": str(e)}
        
        # ====================================================================
        # STAGE 3: Generate Final Report
        # ====================================================================
        logger.info(f"\n\n{'=' * 80}")
        logger.info("📊 TRUSTED ZONE PIPELINE - FINAL SUMMARY REPORT")
        logger.info(f"{'=' * 80}")
        
        total_initial = 0
        total_final = 0
        total_removed = 0
        
        for table_name, stats in pipeline_summary.items():
            if "error" not in stats:
                initial = stats["initial_count"]
                final = stats["final_count"]
                removed = initial - final
                
                total_initial += initial
                total_final += final
                total_removed += removed
                
                logger.info(f"\n{table_name}:")
                logger.info(f"  Initial records: {initial:,}")
                logger.info(f"  Final records: {final:,}")
                logger.info(f"  Records removed: {removed:,}")
                logger.info(f"  Quality score: {(final / initial * 100):.2f}%")
            else:
                logger.error(f"\n{table_name}: ERROR - {stats['error']}")
        
        logger.info(f"\n{'-' * 80}")
        logger.info(f"GLOBAL STATISTICS:")
        logger.info(f"  Total initial records (across all datasets): {total_initial:,}")
        logger.info(f"  Total final records (cleaned): {total_final:,}")
        logger.info(f"  Total records removed: {total_removed:,}")
        if total_initial > 0:
            logger.info(f"  Overall data quality: {(total_final / total_initial * 100):.2f}%")
        logger.info(f"{'=' * 80}")
        
        logger.info(f"\n✓ Pipeline completed successfully!")
        logger.info(f"✓ Cleaned data saved to: {TRUSTED_DB_PATH}")
        logger.info(f"  Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    except Exception as e:
        logger.error(f"\n✗ Pipeline execution failed: {str(e)}", exc_info=True)
        raise
    
    finally:
        # ====================================================================
        # STAGE 4: Cleanup Resources
        # ====================================================================
        if spark is not None:
            try:
                logger.info(f"\n🧹 Closing Spark session...")
                spark.stop()
                logger.info(f"✓ Spark session closed successfully")
            except Exception as e:
                logger.error(f"✗ Error closing Spark session: {str(e)}")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    """
    Main execution point of the Trusted Zone Pipeline.
    """
    import os
    try:
        execute_trusted_zone_pipeline()
        os._exit(0)  # <-- El cierre forzoso mágico en caso de éxito
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        os._exit(1)  # <-- El cierre forzoso en caso de error
