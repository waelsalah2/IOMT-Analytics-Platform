"""
AWS Glue ETL Job
Transforms raw healthcare device data and FHIR observations for analytics
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RAW_BUCKET', 'FHIR_BUCKET', 'OUTPUT_BUCKET'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
RAW_BUCKET = args['RAW_BUCKET']
FHIR_BUCKET = args['FHIR_BUCKET']
OUTPUT_BUCKET = args['OUTPUT_BUCKET']


def process_raw_device_data():
    """Process raw device data from IoT ingestion"""
    print("Processing raw device data...")
    
    # Read raw JSON data from S3
    raw_df = spark.read.json(f"s3://{RAW_BUCKET}/wearables/*/")
    
    if raw_df.count() == 0:
        print("No raw device data found")
        return None
    
    # Flatten nested readings structure
    processed_df = raw_df.select(
        F.col("deviceId"),
        F.col("deviceType"),
        F.col("patientId"),
        F.to_timestamp("timestamp").alias("timestamp"),
        F.col("readings.heartRate").alias("heart_rate"),
        F.col("readings.bloodOxygen").alias("blood_oxygen"),
        F.col("readings.systolicBP").alias("systolic_bp"),
        F.col("readings.diastolicBP").alias("diastolic_bp"),
        F.col("readings.temperature").alias("temperature"),
        F.col("readings.bloodGlucose").alias("blood_glucose"),
        F.col("readings.respiratoryRate").alias("respiratory_rate"),
        F.size(F.col("alerts")).alias("alert_count"),
        F.array_join(F.col("alerts"), ",").alias("alert_types")
    )
    
    # Add derived columns
    processed_df = processed_df.withColumn(
        "date", F.to_date("timestamp")
    ).withColumn(
        "hour", F.hour("timestamp")
    ).withColumn(
        "has_critical_alert", 
        F.when(
            F.col("alert_types").contains("hypertensive_crisis") | 
            F.col("alert_types").contains("hypoxemia") |
            F.col("alert_types").contains("hypoglycemia"),
            True
        ).otherwise(False)
    )
    
    return processed_df


def process_fhir_observations():
    """Process FHIR Observation resources"""
    print("Processing FHIR observations...")
    
    # Read FHIR JSON data
    fhir_df = spark.read.json(f"s3://{FHIR_BUCKET}/observations/*/")
    
    if fhir_df.count() == 0:
        print("No FHIR observations found")
        return None
    
    # Extract components from FHIR structure
    # Explode components array to get individual readings
    exploded_df = fhir_df.select(
        F.col("id").alias("observation_id"),
        F.col("status"),
        F.to_timestamp("effectiveDateTime").alias("effective_timestamp"),
        F.col("subject.reference").alias("patient_reference"),
        F.col("device.identifier.value").alias("device_id"),
        F.explode_outer("component").alias("component")
    )
    
    # Pivot to get readings as columns
    readings_df = exploded_df.select(
        "observation_id",
        "status",
        "effective_timestamp",
        "patient_reference",
        "device_id",
        F.col("component.code.coding")[0]["code"].alias("loinc_code"),
        F.col("component.valueQuantity.value").alias("value"),
        F.col("component.valueQuantity.unit").alias("unit")
    )
    
    # Pivot LOINC codes to columns
    pivoted_df = readings_df.groupBy(
        "observation_id", "status", "effective_timestamp", 
        "patient_reference", "device_id"
    ).pivot("loinc_code").agg(F.first("value"))
    
    # Rename columns to meaningful names
    column_mapping = {
        "8867-4": "heart_rate",
        "59408-5": "blood_oxygen",
        "8480-6": "systolic_bp",
        "8462-4": "diastolic_bp",
        "8310-5": "temperature",
        "2339-0": "blood_glucose",
        "9279-1": "respiratory_rate"
    }
    
    for old_name, new_name in column_mapping.items():
        if old_name in pivoted_df.columns:
            pivoted_df = pivoted_df.withColumnRenamed(old_name, new_name)
    
    return pivoted_df


def create_patient_summary():
    """Create aggregated patient health summary"""
    print("Creating patient health summary...")
    
    fhir_df = process_fhir_observations()
    if fhir_df is None:
        return None
    
    # Aggregate by patient
    summary_df = fhir_df.groupBy("patient_reference").agg(
        F.count("observation_id").alias("total_observations"),
        F.min("effective_timestamp").alias("first_reading"),
        F.max("effective_timestamp").alias("last_reading"),
        F.avg("heart_rate").alias("avg_heart_rate"),
        F.min("heart_rate").alias("min_heart_rate"),
        F.max("heart_rate").alias("max_heart_rate"),
        F.avg("blood_oxygen").alias("avg_blood_oxygen"),
        F.min("blood_oxygen").alias("min_blood_oxygen"),
        F.avg("systolic_bp").alias("avg_systolic_bp"),
        F.avg("diastolic_bp").alias("avg_diastolic_bp"),
        F.avg("blood_glucose").alias("avg_blood_glucose"),
        F.countDistinct("device_id").alias("unique_devices")
    )
    
    # Add health risk indicators
    summary_df = summary_df.withColumn(
        "heart_rate_risk",
        F.when(F.col("avg_heart_rate") > 100, "elevated")
         .when(F.col("avg_heart_rate") < 60, "low")
         .otherwise("normal")
    ).withColumn(
        "oxygen_risk",
        F.when(F.col("avg_blood_oxygen") < 95, "concerning")
         .when(F.col("avg_blood_oxygen") < 92, "critical")
         .otherwise("normal")
    ).withColumn(
        "bp_risk",
        F.when(F.col("avg_systolic_bp") > 140, "hypertension")
         .when(F.col("avg_systolic_bp") < 90, "hypotension")
         .otherwise("normal")
    )
    
    return summary_df


def create_device_analytics():
    """Create device usage and reliability analytics"""
    print("Creating device analytics...")
    
    raw_df = process_raw_device_data()
    if raw_df is None:
        return None
    
    device_df = raw_df.groupBy("deviceId", "deviceType").agg(
        F.count("*").alias("total_readings"),
        F.min("timestamp").alias("first_seen"),
        F.max("timestamp").alias("last_seen"),
        F.countDistinct("patientId").alias("unique_patients"),
        F.sum(F.col("alert_count")).alias("total_alerts"),
        F.sum(F.when(F.col("has_critical_alert"), 1).otherwise(0)).alias("critical_alerts"),
        F.avg("heart_rate").alias("avg_heart_rate_reading"),
        F.stddev("heart_rate").alias("heart_rate_stddev")
    )
    
    # Calculate device health score
    device_df = device_df.withColumn(
        "device_health_score",
        F.least(
            F.lit(100),
            F.greatest(
                F.lit(0),
                F.lit(100) - (F.col("critical_alerts") * 5) - 
                (F.when(F.col("heart_rate_stddev") > 30, 20).otherwise(0))
            )
        )
    )
    
    return device_df


def create_hourly_trends():
    """Create hourly aggregated trends for dashboards"""
    print("Creating hourly trends...")
    
    raw_df = process_raw_device_data()
    if raw_df is None:
        return None
    
    hourly_df = raw_df.groupBy("date", "hour", "deviceType").agg(
        F.count("*").alias("reading_count"),
        F.avg("heart_rate").alias("avg_heart_rate"),
        F.avg("blood_oxygen").alias("avg_blood_oxygen"),
        F.avg("systolic_bp").alias("avg_systolic_bp"),
        F.avg("diastolic_bp").alias("avg_diastolic_bp"),
        F.sum("alert_count").alias("total_alerts"),
        F.countDistinct("deviceId").alias("active_devices"),
        F.countDistinct("patientId").alias("active_patients")
    )
    
    return hourly_df


def write_to_parquet(df, path, partition_cols=None):
    """Write DataFrame to Parquet format with optional partitioning"""
    if df is None:
        print(f"Skipping write to {path} - no data")
        return
    
    writer = df.write.mode("overwrite").format("parquet")
    
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    
    writer.save(f"s3://{OUTPUT_BUCKET}/{path}")
    print(f"Written {df.count()} records to {path}")


# Main ETL execution
print("Starting Healthcare ETL Job")

# Process and write raw device data
raw_data = process_raw_device_data()
write_to_parquet(raw_data, "processed/device_readings", ["date", "deviceType"])

# Process and write FHIR observations
fhir_data = process_fhir_observations()
write_to_parquet(fhir_data, "processed/fhir_observations")

# Create and write patient summaries
patient_summary = create_patient_summary()
write_to_parquet(patient_summary, "analytics/patient_summary")

# Create and write device analytics
device_analytics = create_device_analytics()
write_to_parquet(device_analytics, "analytics/device_analytics")

# Create and write hourly trends
hourly_trends = create_hourly_trends()
write_to_parquet(hourly_trends, "analytics/hourly_trends", ["date"])

print("Healthcare ETL Job completed successfully")

job.commit()
