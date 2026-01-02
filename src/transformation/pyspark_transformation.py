"""
PySpark SWIFT Message Data Analysis
Performs analysis on processed SWIFT data including:
- Daily transaction volumes
- Top senders and receivers
- Average transaction amounts by country
- Anomaly detection for fraud
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, stddev, min as spark_min, 
    max as spark_max, date_trunc, desc, row_number, abs as spark_abs
)
from pyspark.sql.window import Window
from datetime import datetime
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SWIFTDataAnalyzer:
    """
    Performs analysis on processed SWIFT data
    """
    
    def __init__(self):
        """Initialize Spark session"""
        self.spark = SparkSession.builder \
            .appName("SWIFT Data Analysis") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
        
        logger.info("SWIFT Data Analyzer initialized")
    
    def read_processed_data(self, input_path):
        """
        Read processed parquet data from GCS
        
        Args:
            input_path: GCS path to processed data
            
        Returns:
            DataFrame: Processed SWIFT data
        """
        logger.info(f"Reading processed data from {input_path}")
        
        df = self.spark.read.parquet(input_path)
        record_count = df.count()
        
        logger.info(f"Loaded {record_count} records for analysis")
        logger.info(f"Schema: {df.columns}")
        
        return df
    
    def analyze_daily_volumes(self, df):
        """
        Analyze daily transaction volumes by currency
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: Daily volume analysis
        """
        logger.info("Analyzing daily transaction volumes")
        
        # Filter valid records only
        valid_df = df.filter(col("is_valid") == True)
        
        # Group by date and currency
        daily_volumes = valid_df.groupBy(
            date_trunc("day", col("timestamp")).alias("date"),
            col("currency")
        ).agg(
            count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_volume"),
            avg("amount").alias("avg_amount"),
            spark_min("amount").alias("min_amount"),
            spark_max("amount").alias("max_amount")
        ).orderBy("date", "currency")
        
        result_count = daily_volumes.count()
        logger.info(f"Generated {result_count} daily volume records")
        
        # Show sample
        logger.info("Sample daily volumes:")
        daily_volumes.show(10, truncate=False)
        
        return daily_volumes
    
    def analyze_top_senders_receivers(self, df):
        """
        Identify top senders and receivers by transaction count and volume
        
        Args:
            df: Input DataFrame
            
        Returns:
            tuple: (top_senders_df, top_receivers_df)
        """
        logger.info("Analyzing top senders and receivers")
        
        valid_df = df.filter(col("is_valid") == True)
        
        # Top senders
        top_senders = valid_df.groupBy("sender_bic").agg(
            count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_sent"),
            avg("amount").alias("avg_sent")
        ).orderBy(desc("transaction_count")).limit(20)
        
        logger.info("Top 20 senders by transaction count:")
        top_senders.show(20, truncate=False)
        
        # Top receivers
        top_receivers = valid_df.groupBy("receiver_bic").agg(
            count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_received"),
            avg("amount").alias("avg_received")
        ).orderBy(desc("transaction_count")).limit(20)
        
        logger.info("Top 20 receivers by transaction count:")
        top_receivers.show(20, truncate=False)
        
        return top_senders, top_receivers
    
    def analyze_country_statistics(self, df):
        """
        Calculate average transaction amounts by country pairs
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: Country pair statistics
        """
        logger.info("Analyzing transaction statistics by country pairs")
        
        valid_df = df.filter(col("is_valid") == True)
        
        # Extract country codes from BIC (characters 5-6)
        # BIC format: AAAABBCCXXX where BB is country code
        country_stats = valid_df.withColumn(
            "sender_country",
            col("sender_bic").substr(5, 2)
        ).withColumn(
            "receiver_country",
            col("receiver_bic").substr(5, 2)
        ).groupBy("sender_country", "receiver_country", "currency").agg(
            count("*").alias("transaction_count"),
            avg("amount").alias("avg_amount"),
            stddev("amount").alias("stddev_amount"),
            spark_min("amount").alias("min_amount"),
            spark_max("amount").alias("max_amount"),
            spark_sum("amount").alias("total_volume")
        ).orderBy(desc("transaction_count"))
        
        result_count = country_stats.count()
        logger.info(f"Generated {result_count} country pair statistics")
        
        logger.info("Sample country pair statistics:")
        country_stats.show(20, truncate=False)
        
        return country_stats
    
    def detect_anomalies(self, df):
        """
        Detect potentially fraudulent transactions using statistical methods
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: Suspicious transactions with anomaly scores
        """
        logger.info("Performing anomaly detection for fraud")
        
        valid_df = df.filter(col("is_valid") == True)
        
        # Calculate global statistics
        stats = valid_df.select(
            avg("amount").alias("mean_amount"),
            stddev("amount").alias("std_amount")
        ).collect()[0]
        
        mean_amount = stats["mean_amount"]
        std_amount = stats["std_amount"]
        
        logger.info(f"Mean transaction amount: ${mean_amount:,.2f}")
        logger.info(f"Std dev transaction amount: ${std_amount:,.2f}")
        
        # Calculate z-score for amounts
        anomalies = valid_df.withColumn(
            "z_score",
            (col("amount") - mean_amount) / std_amount
        ).withColumn(
            "is_high_value_anomaly",
            col("amount") > (mean_amount + 3 * std_amount)
        ).withColumn(
            "is_statistical_anomaly",
            spark_abs(col("z_score")) > 3
        )
        
        # Detect rapid consecutive transactions from same sender
        window_spec = Window.partitionBy("sender_bic").orderBy("timestamp")
        
        anomalies = anomalies.withColumn(
            "row_num",
            row_number().over(window_spec)
        )
        
        # Calculate time differences between consecutive transactions (simplified detection)
        # In production, you'd use lag() to calculate actual time differences
        
        # Filter to only anomalous transactions
        suspicious = anomalies.filter(
            (col("is_high_value_anomaly") == True) |
            (col("is_statistical_anomaly") == True) |
            (col("is_high_value") == True)
        ).select(
            "timestamp",
            "sender_bic",
            "receiver_bic",
            "amount",
            "currency",
            "z_score",
            "is_high_value_anomaly",
            "is_statistical_anomaly",
            "is_high_value"
        ).orderBy(desc("z_score"))
        
        anomaly_count = suspicious.count()
        logger.info(f"Detected {anomaly_count} potentially fraudulent transactions")
        
        logger.info("Sample suspicious transactions:")
        suspicious.show(20, truncate=False)
        
        return suspicious
    
    def write_analysis_results(self, df, output_path, analysis_name):
        """
        Write analysis results to GCS
        
        Args:
            df: DataFrame to write
            output_path: Base GCS path
            analysis_name: Name of the analysis (subfolder)
        """
        full_path = f"{output_path}/{analysis_name}"
        logger.info(f"Writing {analysis_name} results to {full_path}")
        
        df.write.mode("overwrite").parquet(full_path)
        
        logger.info(f"Successfully wrote {analysis_name} results")
    
    def run_full_analysis(self, input_path, output_path):
        """
        Run complete analysis pipeline
        
        Args:
            input_path: GCS path to processed data
            output_path: GCS path for analysis results
        """
        logger.info("=" * 80)
        logger.info("STARTING SWIFT DATA ANALYSIS PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Input: {input_path}")
        logger.info(f"Output: {output_path}")
        logger.info("=" * 80)
        
        # Read data
        df = self.read_processed_data(input_path)
        
        # Run analyses
        logger.info("\n" + "=" * 80)
        logger.info("1. DAILY TRANSACTION VOLUMES")
        logger.info("=" * 80)
        daily_volumes = self.analyze_daily_volumes(df)
        self.write_analysis_results(daily_volumes, output_path, "daily_volumes")
        
        logger.info("\n" + "=" * 80)
        logger.info("2. TOP SENDERS AND RECEIVERS")
        logger.info("=" * 80)
        top_senders, top_receivers = self.analyze_top_senders_receivers(df)
        self.write_analysis_results(top_senders, output_path, "top_senders")
        self.write_analysis_results(top_receivers, output_path, "top_receivers")
        
        logger.info("\n" + "=" * 80)
        logger.info("3. COUNTRY STATISTICS")
        logger.info("=" * 80)
        country_stats = self.analyze_country_statistics(df)
        self.write_analysis_results(country_stats, output_path, "country_statistics")
        
        logger.info("\n" + "=" * 80)
        logger.info("4. ANOMALY DETECTION")
        logger.info("=" * 80)
        anomalies = self.detect_anomalies(df)
        self.write_analysis_results(anomalies, output_path, "fraud_anomalies")
        
        logger.info("\n" + "=" * 80)
        logger.info("ANALYSIS PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Results saved to: {output_path}")
        logger.info("Analysis outputs:")
        logger.info(f"  - {output_path}/daily_volumes/")
        logger.info(f"  - {output_path}/top_senders/")
        logger.info(f"  - {output_path}/top_receivers/")
        logger.info(f"  - {output_path}/country_statistics/")
        logger.info(f"  - {output_path}/fraud_anomalies/")
        logger.info("=" * 80)
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
        logger.info("Spark session stopped")


def main():
    """
    Main entry point for command-line execution
    """
    if len(sys.argv) != 3:
        print("Usage: python pyspark_transformation.py <input_gcs_path> <output_gcs_path>")
        print("Example: python pyspark_transformation.py gs://bucket/data/processed/swift gs://bucket/data/analysis")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    analyzer = SWIFTDataAnalyzer()
    
    try:
        analyzer.run_full_analysis(input_path, output_path)
        
        print("\n" + "=" * 80)
        print("✓ SUCCESS! Analysis completed successfully")
        print("=" * 80)
        print(f"View your results at: {output_path}")
        print("=" * 80)
        
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        analyzer.stop()


if __name__ == "__main__":
    main()
