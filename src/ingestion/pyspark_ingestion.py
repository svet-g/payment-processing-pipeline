"""
PySpark SWIFT Message Data Ingestion and Processing
Reads, validates, transforms, and performs quality checks on SWIFT message data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, regexp_replace, trim, length,
    when, lit, current_timestamp, year, month, dayofmonth,
    count, sum as spark_sum, avg, stddev, min as spark_min, max as spark_max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from datetime import datetime
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SWIFTDataIngestor:
    """
    Handles ingestion and processing of SWIFT message data
    """
    
    # Valid ISO 4217 currency codes (subset)
    VALID_CURRENCIES = [
        'USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 
        'CHF', 'CNY', 'SEK', 'NZD', 'INR', 'BRL'
    ]
    
    # Valid SWIFT message types
    VALID_MESSAGE_TYPES = ['MT103', 'MT202', 'MT950', 'MT940']
    
    # BIC code pattern: 4 letters (bank) + 2 letters (country) + 2 alphanumeric (location) + optional 3 alphanumeric (branch)
    BIC_PATTERN = r'^[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?$'
    
    def __init__(self, spark_session=None):
        """
        Initialize the ingestor with a Spark session
        
        Args:
            spark_session: Existing SparkSession or None to create new one
        """
        if spark_session:
            self.spark = spark_session
        else:
            self.spark = SparkSession.builder \
                .appName("SWIFT Data Ingestion") \
                .config("spark.sql.session.timeZone", "UTC") \
                .getOrCreate()
        
        logger.info("SWIFT Data Ingestor initialized")
    
    def define_schema(self):
        """
        Define explicit schema for SWIFT message data
        
        Returns:
            StructType: PySpark schema definition
        """
        return StructType([
            StructField("timestamp", StringType(), True),
            StructField("sender_bic", StringType(), True),
            StructField("receiver_bic", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("msg_type", StringType(), True)
        ])
    
    def read_csv(self, input_path):
        """
        Read SWIFT message data from CSV file
        
        Args:
            input_path: Path to input CSV file
            
        Returns:
            DataFrame: Raw data
        """
        logger.info(f"Reading data from {input_path}")
        
        schema = self.define_schema()
        
        df = self.spark.read \
            .option("header", "true") \
            .option("mode", "PERMISSIVE") \
            .schema(schema) \
            .csv(input_path)
        
        record_count = df.count()
        logger.info(f"Loaded {record_count} records")
        
        return df
    
    def transform_data(self, df):
        """
        Transform and parse raw data
        
        Args:
            df: Raw DataFrame
            
        Returns:
            DataFrame: Transformed data
        """
        logger.info("Transforming data")
        
        # Convert timestamp string to proper timestamp
        df = df.withColumn(
            "timestamp",
            to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSS")
        )
        
        # Clean BIC codes (trim whitespace, uppercase)
        df = df.withColumn("sender_bic", trim(col("sender_bic"))) \
               .withColumn("receiver_bic", trim(col("receiver_bic"))) \
               .withColumn("currency", trim(col("currency"))) \
               .withColumn("msg_type", trim(col("msg_type")))
        
        # Add processing metadata
        df = df.withColumn("processing_timestamp", current_timestamp()) \
               .withColumn("year", year(col("timestamp"))) \
               .withColumn("month", month(col("timestamp"))) \
               .withColumn("day", dayofmonth(col("timestamp")))
        
        return df
    
    def validate_data(self, df):
        """
        Perform data quality checks and add validation flags
        
        Args:
            df: Transformed DataFrame
            
        Returns:
            DataFrame: Data with validation flags
        """
        logger.info("Performing data quality checks")
        
        # Check for null values in critical fields
        df = df.withColumn(
            "has_nulls",
            when(
                col("timestamp").isNull() | 
                col("sender_bic").isNull() | 
                col("receiver_bic").isNull() | 
                col("amount").isNull() | 
                col("currency").isNull(),
                lit(True)
            ).otherwise(lit(False))
        )
        
        # Validate amount is positive
        df = df.withColumn(
            "is_amount_valid",
            when(col("amount") > 0, lit(True)).otherwise(lit(False))
        )
        
        # Validate BIC format
        df = df.withColumn(
            "is_sender_bic_valid",
            col("sender_bic").rlike(self.BIC_PATTERN)
        )
        
        df = df.withColumn(
            "is_receiver_bic_valid",
            col("receiver_bic").rlike(self.BIC_PATTERN)
        )
        
        # Validate currency code
        df = df.withColumn(
            "is_currency_valid",
            col("currency").isin(self.VALID_CURRENCIES)
        )
        
        # Validate message type
        df = df.withColumn(
            "is_msg_type_valid",
            col("msg_type").isin(self.VALID_MESSAGE_TYPES)
        )
        
        # Check for future timestamps
        df = df.withColumn(
            "is_timestamp_valid",
            col("timestamp") <= current_timestamp()
        )
        
        # Flag high-value transactions (> $10M)
        df = df.withColumn(
            "is_high_value",
            when(col("amount") > 10000000, lit(True)).otherwise(lit(False))
        )
        
        # Calculate overall quality score
        df = df.withColumn(
            "quality_score",
            (
                (~col("has_nulls")).cast("int") +
                col("is_amount_valid").cast("int") +
                col("is_sender_bic_valid").cast("int") +
                col("is_receiver_bic_valid").cast("int") +
                col("is_currency_valid").cast("int") +
                col("is_msg_type_valid").cast("int") +
                col("is_timestamp_valid").cast("int")
            ) / 7.0
        )
        
        # Mark record as valid if quality score >= threshold
        df = df.withColumn(
            "is_valid",
            when(col("quality_score") >= 0.85, lit(True)).otherwise(lit(False))
        )
        
        return df
    
    def generate_quality_report(self, df):
        """
        Generate data quality report
        
        Args:
            df: Validated DataFrame
            
        Returns:
            dict: Quality metrics
        """
        logger.info("Generating quality report")
        
        total_records = df.count()
        valid_records = df.filter(col("is_valid")).count()
        invalid_records = total_records - valid_records
        
        # Count specific validation failures
        null_count = df.filter(col("has_nulls")).count()
        invalid_amount_count = df.filter(~col("is_amount_valid")).count()
        invalid_sender_bic_count = df.filter(~col("is_sender_bic_valid")).count()
        invalid_receiver_bic_count = df.filter(~col("is_receiver_bic_valid")).count()
        invalid_currency_count = df.filter(~col("is_currency_valid")).count()
        invalid_msg_type_count = df.filter(~col("is_msg_type_valid")).count()
        future_timestamp_count = df.filter(~col("is_timestamp_valid")).count()
        high_value_count = df.filter(col("is_high_value")).count()
        
        # Calculate average quality score
        avg_quality = df.agg(avg(col("quality_score"))).collect()[0][0]
        
        report = {
            "total_records": total_records,
            "valid_records": valid_records,
            "invalid_records": invalid_records,
            "validity_rate": valid_records / total_records if total_records > 0 else 0,
            "avg_quality_score": float(avg_quality) if avg_quality else 0.0,
            "null_count": null_count,
            "invalid_amount_count": invalid_amount_count,
            "invalid_sender_bic_count": invalid_sender_bic_count,
            "invalid_receiver_bic_count": invalid_receiver_bic_count,
            "invalid_currency_count": invalid_currency_count,
            "invalid_msg_type_count": invalid_msg_type_count,
            "future_timestamp_count": future_timestamp_count,
            "high_value_count": high_value_count
        }
        
        # Log report
        logger.info("=" * 60)
        logger.info("DATA QUALITY REPORT")
        logger.info("=" * 60)
        logger.info(f"Total Records: {report['total_records']}")
        logger.info(f"Valid Records: {report['valid_records']}")
        logger.info(f"Invalid Records: {report['invalid_records']}")
        logger.info(f"Validity Rate: {report['validity_rate']:.2%}")
        logger.info(f"Avg Quality Score: {report['avg_quality_score']:.3f}")
        logger.info("-" * 60)
        logger.info(f"Records with Nulls: {report['null_count']}")
        logger.info(f"Invalid Amounts: {report['invalid_amount_count']}")
        logger.info(f"Invalid Sender BICs: {report['invalid_sender_bic_count']}")
        logger.info(f"Invalid Receiver BICs: {report['invalid_receiver_bic_count']}")
        logger.info(f"Invalid Currencies: {report['invalid_currency_count']}")
        logger.info(f"Invalid Message Types: {report['invalid_msg_type_count']}")
        logger.info(f"Future Timestamps: {report['future_timestamp_count']}")
        logger.info(f"High Value Transactions (>$10M): {report['high_value_count']}")
        logger.info("=" * 60)
        
        return report
    
    def perform_aggregations(self, df):
        """
        Perform basic aggregations on valid data
        
        Args:
            df: Validated DataFrame
            
        Returns:
            dict: Aggregation results
        """
        logger.info("Performing aggregations")
        
        # Filter to valid records only
        valid_df = df.filter(col("is_valid"))
        
        # Overall statistics
        stats = valid_df.agg(
            count("*").alias("total_transactions"),
            spark_sum("amount").alias("total_volume"),
            avg("amount").alias("avg_amount"),
            stddev("amount").alias("stddev_amount"),
            spark_min("amount").alias("min_amount"),
            spark_max("amount").alias("max_amount")
        ).collect()[0]
        
        # Currency breakdown
        currency_stats = valid_df.groupBy("currency") \
            .agg(
                count("*").alias("transaction_count"),
                spark_sum("amount").alias("total_volume")
            ) \
            .orderBy(col("transaction_count").desc()) \
            .collect()
        
        # Message type breakdown
        msg_type_stats = valid_df.groupBy("msg_type") \
            .agg(count("*").alias("transaction_count")) \
            .orderBy(col("transaction_count").desc()) \
            .collect()
        
        aggregations = {
            "overall": {
                "total_transactions": stats["total_transactions"],
                "total_volume": float(stats["total_volume"]) if stats["total_volume"] else 0.0,
                "avg_amount": float(stats["avg_amount"]) if stats["avg_amount"] else 0.0,
                "stddev_amount": float(stats["stddev_amount"]) if stats["stddev_amount"] else 0.0,
                "min_amount": float(stats["min_amount"]) if stats["min_amount"] else 0.0,
                "max_amount": float(stats["max_amount"]) if stats["max_amount"] else 0.0
            },
            "by_currency": [
                {
                    "currency": row["currency"],
                    "transaction_count": row["transaction_count"],
                    "total_volume": float(row["total_volume"])
                }
                for row in currency_stats
            ],
            "by_message_type": [
                {
                    "msg_type": row["msg_type"],
                    "transaction_count": row["transaction_count"]
                }
                for row in msg_type_stats
            ]
        }
        
        # Log aggregations
        logger.info("=" * 60)
        logger.info("AGGREGATION RESULTS")
        logger.info("=" * 60)
        logger.info(f"Total Transactions: {aggregations['overall']['total_transactions']}")
        logger.info(f"Total Volume: ${aggregations['overall']['total_volume']:,.2f}")
        logger.info(f"Average Amount: ${aggregations['overall']['avg_amount']:,.2f}")
        logger.info(f"Std Dev: ${aggregations['overall']['stddev_amount']:,.2f}")
        logger.info(f"Min Amount: ${aggregations['overall']['min_amount']:,.2f}")
        logger.info(f"Max Amount: ${aggregations['overall']['max_amount']:,.2f}")
        logger.info("=" * 60)
        
        return aggregations
    
    def write_parquet(self, df, output_path, partition_by=None):
        """
        Write processed data to Parquet format
        
        Args:
            df: DataFrame to write
            output_path: Output path
            partition_by: List of columns to partition by
        """
        logger.info(f"Writing data to {output_path}")
        
        writer = df.write.mode("overwrite")
        
        if partition_by:
            writer = writer.partitionBy(partition_by)
        
        writer.parquet(output_path)
        logger.info("Data written successfully")
    
    def process(self, input_path, output_path, partition_by=None):
        """
        Main processing pipeline
        
        Args:
            input_path: Input CSV path
            output_path: Output Parquet path
            partition_by: Columns to partition by
            
        Returns:
            tuple: (DataFrame, quality_report, aggregations)
        """
        logger.info("Starting SWIFT data processing pipeline")
        
        # Read data
        df = self.read_csv(input_path)
        
        # Transform
        df = self.transform_data(df)
        
        # Validate
        df = self.validate_data(df)
        
        # Generate reports
        quality_report = self.generate_quality_report(df)
        aggregations = self.perform_aggregations(df)
        
        # Write output
        self.write_parquet(df, output_path, partition_by)
        
        logger.info("Processing pipeline completed successfully")
        
        return df, quality_report, aggregations
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
        logger.info("Spark session stopped")


def main():
    """
    Main entry point for command-line execution
    """
    if len(sys.argv) < 3:
        print("Usage: python pyspark_ingestion.py <input_csv_path> <output_parquet_path> [partition_columns]")
        print("Example: python pyspark_ingestion.py gs://payment-processing-pipeline-svet-g/raw/swift_messages.csv gs://payment-processing-pipeline-svet-g/processed/swift")
        print("Example with partitioning: python pyspark_ingestion.py gs://payment-processing-pipeline-svet-g/raw/swift_messages.csv gs://payment-processing-pipeline-svet-g/processed/swift year,month")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    partition_by = sys.argv[3].split(',') if len(sys.argv) > 3 else ['year', 'month', 'day']
    
    # Initialize and run
    ingestor = SWIFTDataIngestor()
    
    try:
        df, quality_report, aggregations = ingestor.process(
            input_path=input_path,
            output_path=output_path,
            partition_by=partition_by
        )
        
        print("\n" + "=" * 60)
        print("PROCESSING SUMMARY")
        print("=" * 60)
        print(f"✓ Processed {quality_report['total_records']} records")
        print(f"✓ Validity Rate: {quality_report['validity_rate']:.2%}")
        print(f"✓ Output written to: {output_path}")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        ingestor.stop()


if __name__ == "__main__":
    main()
