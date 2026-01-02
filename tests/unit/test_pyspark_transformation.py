"""
Unit tests for SWIFT Data Analyzer
Tests the analysis functionality using mocked Spark DataFrames
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, BooleanType
)
import sys
import os

# Add parent directory to path to import the module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))

from transformation.pyspark_transformation import SWIFTDataAnalyzer


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("SWIFT_Test") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_schema():
    """Define the schema for test data"""
    return StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("sender_bic", StringType(), True),
        StructField("receiver_bic", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("is_valid", BooleanType(), True),
        StructField("is_high_value", BooleanType(), True)
    ])


@pytest.fixture
def sample_data(spark, sample_schema):
    """Create sample test data"""
    data = [
        (datetime(2024, 1, 1, 10, 0), "AAAAUSBBXXX", "BBBBGBCCXXX", 1000.0, "USD", True, False),
        (datetime(2024, 1, 1, 11, 0), "AAAAUSBBXXX", "CCCCFRDDXXX", 2000.0, "EUR", True, False),
        (datetime(2024, 1, 2, 10, 0), "BBBBGBCCXXX", "AAAAUSBBXXX", 5000.0, "USD", True, False),
        (datetime(2024, 1, 2, 11, 0), "CCCCFRDDXXX", "BBBBGBCCXXX", 100000.0, "EUR", True, True),
        (datetime(2024, 1, 3, 10, 0), "AAAAUSBBXXX", "DDDDDEEEYYY", 1500.0, "USD", True, False),
        (datetime(2024, 1, 3, 11, 0), "INVALID_BIC", "BBBBGBCCXXX", 3000.0, "USD", False, False),
    ]
    
    return spark.createDataFrame(data, schema=sample_schema)


@pytest.fixture
def analyzer():
    """Create analyzer instance with mocked Spark session"""
    with patch('transformation.pyspark_transformation.SparkSession') as mock_spark:
        analyzer = SWIFTDataAnalyzer()
        return analyzer


class TestSWIFTDataAnalyzer:
    """Test suite for SWIFTDataAnalyzer class"""
    
    def test_initialization(self, analyzer):
        """Test analyzer initialization"""
        assert analyzer.spark is not None
        
    def test_read_processed_data(self, analyzer, sample_data):
        """Test reading processed data from GCS"""
        analyzer.spark.read.parquet = Mock(return_value=sample_data)
        
        df = analyzer.read_processed_data("gs://test-bucket/data")
        
        assert df is not None
        assert df.count() == 6
        analyzer.spark.read.parquet.assert_called_once_with("gs://test-bucket/data")
    
    def test_analyze_daily_volumes(self, spark, sample_data):
        """Test daily volume analysis"""
        # Use real spark session for this test
        analyzer = SWIFTDataAnalyzer()
        analyzer.spark = spark
        
        daily_volumes = analyzer.analyze_daily_volumes(sample_data)
        
        # Should filter out invalid records
        assert daily_volumes.count() > 0
        
        # Check columns exist
        expected_cols = ["date", "currency", "transaction_count", 
                        "total_volume", "avg_amount", "min_amount", "max_amount"]
        assert all(col in daily_volumes.columns for col in expected_cols)
        
        # Verify at least one USD record exists
        usd_records = daily_volumes.filter(daily_volumes.currency == "USD")
        assert usd_records.count() > 0
    
    def test_analyze_top_senders_receivers(self, spark, sample_data):
        """Test top senders and receivers analysis"""
        analyzer = SWIFTDataAnalyzer()
        analyzer.spark = spark
        
        top_senders, top_receivers = analyzer.analyze_top_senders_receivers(sample_data)
        
        # Check that both DataFrames are returned
        assert top_senders is not None
        assert top_receivers is not None
        
        # Check columns exist
        sender_cols = ["sender_bic", "transaction_count", "total_sent", "avg_sent"]
        receiver_cols = ["receiver_bic", "transaction_count", "total_received", "avg_received"]
        
        assert all(col in top_senders.columns for col in sender_cols)
        assert all(col in top_receivers.columns for col in receiver_cols)
        
        # Verify AAAAUSBBXXX is in top senders (appears 3 times in valid data)
        sender_bics = [row.sender_bic for row in top_senders.collect()]
        assert "AAAAUSBBXXX" in sender_bics
    
    def test_analyze_country_statistics(self, spark, sample_data):
        """Test country statistics analysis"""
        analyzer = SWIFTDataAnalyzer()
        analyzer.spark = spark
        
        country_stats = analyzer.analyze_country_statistics(sample_data)
        
        assert country_stats is not None
        assert country_stats.count() > 0
        
        # Check columns exist
        expected_cols = ["sender_country", "receiver_country", "currency",
                        "transaction_count", "avg_amount", "stddev_amount",
                        "min_amount", "max_amount", "total_volume"]
        assert all(col in country_stats.columns for col in expected_cols)
        
        # Verify country code extraction (position 5-6 in BIC)
        # AAAAUSBBXXX -> US, BBBBGBCCXXX -> GB
        stats_list = country_stats.collect()
        countries = [(row.sender_country, row.receiver_country) for row in stats_list]
        assert ("US", "GB") in countries or ("US", "FR") in countries
    
    def test_detect_anomalies(self, spark, sample_data):
        """Test anomaly detection"""
        analyzer = SWIFTDataAnalyzer()
        analyzer.spark = spark
        
        anomalies = analyzer.detect_anomalies(sample_data)
        
        assert anomalies is not None
        
        # Check columns exist
        expected_cols = ["timestamp", "sender_bic", "receiver_bic", "amount",
                        "currency", "z_score", "is_high_value_anomaly",
                        "is_statistical_anomaly", "is_high_value"]
        assert all(col in anomalies.columns for col in expected_cols)
        
        # The 100,000 EUR transaction should be flagged
        anomaly_list = anomalies.collect()
        assert len(anomaly_list) > 0
        
        # Check that high value transaction is included
        amounts = [row.amount for row in anomaly_list]
        assert 100000.0 in amounts
    
    def test_write_analysis_results(self, analyzer):
        """Test writing analysis results"""
        mock_df = Mock()
        mock_df.write.mode.return_value.parquet = Mock()
        
        analyzer.write_analysis_results(mock_df, "gs://bucket/output", "test_analysis")
        
        mock_df.write.mode.assert_called_once_with("overwrite")
        mock_df.write.mode.return_value.parquet.assert_called_once_with(
            "gs://bucket/output/test_analysis"
        )
    
    def test_filters_invalid_records(self, spark, sample_data):
        """Test that invalid records are filtered out"""
        analyzer = SWIFTDataAnalyzer()
        analyzer.spark = spark
        
        # Daily volumes should only include valid records
        daily_volumes = analyzer.analyze_daily_volumes(sample_data)
        
        # Original data has 6 records, 1 invalid
        # Valid data has 5 records
        total_transactions = daily_volumes.select("transaction_count").rdd.map(
            lambda r: r[0]
        ).sum()
        
        assert total_transactions == 5  # Should exclude the invalid record
    
    def test_run_full_analysis(self, analyzer, sample_data):
        """Test complete analysis pipeline"""
        analyzer.read_processed_data = Mock(return_value=sample_data)
        analyzer.write_analysis_results = Mock()
        
        analyzer.run_full_analysis("gs://input", "gs://output")
        
        # Verify read was called
        analyzer.read_processed_data.assert_called_once_with("gs://input")
        
        # Verify write was called 5 times (for each analysis)
        assert analyzer.write_analysis_results.call_count == 5
        
        # Verify the correct analysis names
        call_args = [call[0][2] for call in analyzer.write_analysis_results.call_args_list]
        expected_analyses = [
            "daily_volumes",
            "top_senders",
            "top_receivers",
            "country_statistics",
            "fraud_anomalies"
        ]
        assert call_args == expected_analyses
    
    def test_stop(self, analyzer):
        """Test Spark session stop"""
        analyzer.spark.stop = Mock()
        analyzer.stop()
        analyzer.spark.stop.assert_called_once()


class TestMainFunction:
    """Test the main entry point"""
    
    @patch('transformation.pyspark_transformation.SWIFTDataAnalyzer')
    def test_main_with_correct_args(self, mock_analyzer_class):
        mock_analyzer = Mock()
        mock_analyzer_class.return_value = mock_analyzer

        test_args = ['pyspark_transformation.py', 'gs://input', 'gs://output']

        with patch('sys.argv', test_args):
            from transformation.pyspark_transformation import main
            main()

        mock_analyzer_class.assert_called_once()
        mock_analyzer.run_full_analysis.assert_called_once_with(
            'gs://input', 'gs://output'
        )

    
    def test_main_with_incorrect_args(self):
        """Test main function with incorrect arguments"""
        from transformation.pyspark_transformation import main
        
        test_args = ['pyspark_transformation.py']  # Missing required arguments
        
        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()
            
            assert exc_info.value.code == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])