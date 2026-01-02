"""
Test Suite for SWIFT Data Ingestion Module
Tests data validation, transformation, and quality checks
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from datetime import datetime, timedelta
import tempfile
import os
import sys

# Add parent directory to path to import the module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))

from ingestion.pyspark_ingestion import SWIFTDataIngestor


@pytest.fixture(scope="session")
def spark():
    """
    Create a Spark session for testing
    """
    spark = SparkSession.builder \
        .appName("SWIFT Data Ingestion Tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def ingestor(spark):
    """
    Create an ingestor instance with test Spark session
    """
    return SWIFTDataIngestor(spark_session=spark)


@pytest.fixture
def sample_valid_data(spark):
    """
    Create a sample DataFrame with valid SWIFT data
    """
    data = [
        ("2024-12-01 10:30:00.000000", "BOFAUS3NXXX", "CHASUS33XXX", 50000.00, "USD", "MT103"),
        ("2024-12-01 11:45:00.000000", "DEUTDEFFXXX", "BARCGB22XXX", 75000.00, "EUR", "MT202"),
        ("2024-12-01 14:20:00.000000", "BNPAFRPPXXX", "BOFAUS3NXXX", 120000.00, "GBP", "MT950"),
        ("2024-12-02 09:15:00.000000", "CHASUS33XXX", "DEUTDEFFXXX", 30000.00, "USD", "MT103"),
        ("2024-12-02 16:30:00.000000", "BARCGB22XXX", "BNPAFRPPXXX", 95000.00, "EUR", "MT202"),
    ]
    
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("sender_bic", StringType(), True),
        StructField("receiver_bic", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("msg_type", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_invalid_data(spark):
    """
    Create a sample DataFrame with various data quality issues
    """
    future_date = (datetime.now() + timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S.%f")
    
    data = [
        # Valid record
        ("2024-12-01 10:30:00.000000", "BOFAUS3NXXX", "CHASUS33XXX", 50000.00, "USD", "MT103"),
        # Null amount
        ("2024-12-01 11:00:00.000000", "BOFAUS3NXXX", "CHASUS33XXX", None, "USD", "MT103"),
        # Negative amount
        ("2024-12-01 12:00:00.000000", "BOFAUS3NXXX", "CHASUS33XXX", -1000.00, "USD", "MT103"),
        # Invalid sender BIC
        ("2024-12-01 13:00:00.000000", "INVALID123", "CHASUS33XXX", 5000.00, "USD", "MT103"),
        # Invalid currency
        ("2024-12-01 14:00:00.000000", "BOFAUS3NXXX", "CHASUS33XXX", 5000.00, "XYZ", "MT103"),
        # Future timestamp
        (future_date, "BOFAUS3NXXX", "CHASUS33XXX", 5000.00, "USD", "MT103"),
        # High value transaction
        ("2024-12-01 15:00:00.000000", "BOFAUS3NXXX", "CHASUS33XXX", 15000000.00, "USD", "MT103"),
        # Invalid message type
        ("2024-12-01 16:00:00.000000", "BOFAUS3NXXX", "CHASUS33XXX", 5000.00, "USD", "MT999"),
    ]
    
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("sender_bic", StringType(), True),
        StructField("receiver_bic", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("msg_type", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def temp_csv_file(sample_valid_data):
    """
    Create a temporary CSV file with sample data
    """
    temp_dir = tempfile.mkdtemp()
    csv_path = os.path.join(temp_dir, "test_swift_data.csv")
    
    # Write to CSV
    sample_valid_data.toPandas().to_csv(csv_path, index=False)
    
    yield csv_path
    
    # Cleanup
    if os.path.exists(csv_path):
        os.remove(csv_path)
    os.rmdir(temp_dir)


class TestSWIFTDataIngestor:
    """
    Test suite for SWIFT Data Ingestor
    """
    
    def test_ingestor_initialization(self, spark):
        """Test that ingestor initializes correctly"""
        ingestor = SWIFTDataIngestor(spark_session=spark)
        assert ingestor.spark is not None
        assert ingestor.spark == spark
    
    def test_schema_definition(self, ingestor):
        """Test that schema is correctly defined"""
        schema = ingestor.define_schema()
        assert len(schema.fields) == 6
        assert schema.fieldNames() == [
            "timestamp", "sender_bic", "receiver_bic", 
            "amount", "currency", "msg_type"
        ]
    
    def test_read_csv(self, ingestor, temp_csv_file):
        """Test CSV reading functionality"""
        df = ingestor.read_csv(temp_csv_file)
        assert df is not None
        assert df.count() == 5
        assert len(df.columns) == 6
    
    def test_transform_data(self, ingestor, sample_valid_data):
        """Test data transformation"""
        transformed_df = ingestor.transform_data(sample_valid_data)
        
        # Check that new columns are added
        assert "processing_timestamp" in transformed_df.columns
        assert "year" in transformed_df.columns
        assert "month" in transformed_df.columns
        assert "day" in transformed_df.columns
        
        # Check timestamp conversion
        first_row = transformed_df.first()
        assert first_row["timestamp"] is not None
    
    def test_validate_data_valid_records(self, ingestor, sample_valid_data):
        """Test validation with all valid records"""
        transformed_df = ingestor.transform_data(sample_valid_data)
        validated_df = ingestor.validate_data(transformed_df)
        
        # All records should be valid
        valid_count = validated_df.filter(validated_df.is_valid).count()
        assert valid_count == sample_valid_data.count()
        
        # Check validation columns exist
        assert "has_nulls" in validated_df.columns
        assert "is_amount_valid" in validated_df.columns
        assert "is_sender_bic_valid" in validated_df.columns
        assert "is_receiver_bic_valid" in validated_df.columns
        assert "is_currency_valid" in validated_df.columns
        assert "is_msg_type_valid" in validated_df.columns
        assert "is_timestamp_valid" in validated_df.columns
        assert "quality_score" in validated_df.columns
        assert "is_valid" in validated_df.columns
    
    def test_validate_data_null_detection(self, ingestor, spark):
        """Test that null values are detected"""
        data = [("2024-12-01 10:30:00.000000", "BOFAUS3NXXX", "CHASUS33XXX", None, "USD", "MT103")]
        schema = ingestor.define_schema()
        df = spark.createDataFrame(data, schema)
        
        transformed_df = ingestor.transform_data(df)
        validated_df = ingestor.validate_data(transformed_df)
        
        # Should detect null
        assert validated_df.filter(validated_df.has_nulls).count() == 1
        assert validated_df.filter(validated_df.is_valid).count() == 0
    
    def test_validate_data_negative_amount(self, ingestor, spark):
        """Test that negative amounts are flagged"""
        data = [("2024-12-01 10:30:00.000000", "BOFAUS3NXXX", "CHASUS33XXX", -1000.00, "USD", "MT103")]
        schema = ingestor.define_schema()
        df = spark.createDataFrame(data, schema)
        
        transformed_df = ingestor.transform_data(df)
        validated_df = ingestor.validate_data(transformed_df)
        
        # Should flag invalid amount
        assert validated_df.filter(~validated_df.is_amount_valid).count() == 1
    
    def test_validate_data_invalid_bic(self, ingestor, spark):
        """Test that invalid BIC codes are detected"""
        data = [("2024-12-01 10:30:00.000000", "INVALID", "CHASUS33XXX", 1000.00, "USD", "MT103")]
        schema = ingestor.define_schema()
        df = spark.createDataFrame(data, schema)
        
        transformed_df = ingestor.transform_data(df)
        validated_df = ingestor.validate_data(transformed_df)
        
        # Should flag invalid sender BIC
        assert validated_df.filter(~validated_df.is_sender_bic_valid).count() == 1
    
    def test_validate_data_invalid_currency(self, ingestor, spark):
        """Test that invalid currency codes are detected"""
        data = [("2024-12-01 10:30:00.000000", "BOFAUS3NXXX", "CHASUS33XXX", 1000.00, "XXX", "MT103")]
        schema = ingestor.define_schema()
        df = spark.createDataFrame(data, schema)
        
        transformed_df = ingestor.transform_data(df)
        validated_df = ingestor.validate_data(transformed_df)
        
        # Should flag invalid currency
        assert validated_df.filter(~validated_df.is_currency_valid).count() == 1
    
    def test_validate_data_future_timestamp(self, ingestor, spark):
        """Test that future timestamps are detected"""
        future_date = (datetime.now() + timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S.%f")
        data = [(future_date, "BOFAUS3NXXX", "CHASUS33XXX", 1000.00, "USD", "MT103")]
        schema = ingestor.define_schema()
        df = spark.createDataFrame(data, schema)
        
        transformed_df = ingestor.transform_data(df)
        validated_df = ingestor.validate_data(transformed_df)
        
        # Should flag future timestamp
        assert validated_df.filter(~validated_df.is_timestamp_valid).count() == 1
    
    def test_validate_data_high_value_detection(self, ingestor, spark):
        """Test that high value transactions are flagged"""
        data = [("2024-12-01 10:30:00.000000", "BOFAUS3NXXX", "CHASUS33XXX", 15000000.00, "USD", "MT103")]
        schema = ingestor.define_schema()
        df = spark.createDataFrame(data, schema)
        
        transformed_df = ingestor.transform_data(df)
        validated_df = ingestor.validate_data(transformed_df)
        
        # Should flag as high value
        assert validated_df.filter(validated_df.is_high_value).count() == 1
    
    def test_validate_data_quality_score(self, ingestor, sample_valid_data):
        """Test quality score calculation"""
        transformed_df = ingestor.transform_data(sample_valid_data)
        validated_df = ingestor.validate_data(transformed_df)
        
        # All valid records should have quality score of 1.0
        scores = validated_df.select("quality_score").collect()
        for row in scores:
            assert row["quality_score"] == 1.0
    
    def test_generate_quality_report(self, ingestor, sample_invalid_data):
        """Test quality report generation"""
        transformed_df = ingestor.transform_data(sample_invalid_data)
        validated_df = ingestor.validate_data(transformed_df)
        
        report = ingestor.generate_quality_report(validated_df)
        
        # Check report structure
        assert "total_records" in report
        assert "valid_records" in report
        assert "invalid_records" in report
        assert "validity_rate" in report
        assert "avg_quality_score" in report
        
        # Check that some records are invalid
        assert report["total_records"] == 8
        assert report["invalid_records"] > 0
        assert report["validity_rate"] < 1.0
    
    def test_perform_aggregations(self, ingestor, sample_valid_data):
        """Test aggregation calculations"""
        transformed_df = ingestor.transform_data(sample_valid_data)
        validated_df = ingestor.validate_data(transformed_df)
        
        aggregations = ingestor.perform_aggregations(validated_df)
        
        # Check structure
        assert "overall" in aggregations
        assert "by_currency" in aggregations
        assert "by_message_type" in aggregations
        
        # Check overall stats
        overall = aggregations["overall"]
        assert overall["total_transactions"] == 5
        assert overall["total_volume"] > 0
        assert overall["avg_amount"] > 0
        assert overall["min_amount"] > 0
        assert overall["max_amount"] > 0
        
        # Check currency breakdown
        assert len(aggregations["by_currency"]) > 0
        assert all("currency" in item for item in aggregations["by_currency"])
        assert all("transaction_count" in item for item in aggregations["by_currency"])
    
    def test_write_parquet(self, ingestor, sample_valid_data):
        """Test Parquet writing"""
        temp_dir = tempfile.mkdtemp()
        output_path = os.path.join(temp_dir, "test_output")
        
        try:
            transformed_df = ingestor.transform_data(sample_valid_data)
            validated_df = ingestor.validate_data(transformed_df)
            
            ingestor.write_parquet(validated_df, output_path, partition_by=["year", "month"])
            
            # Verify output exists
            assert os.path.exists(output_path)
            
            # Try reading back
            read_df = ingestor.spark.read.parquet(output_path)
            assert read_df.count() == sample_valid_data.count()
            
        finally:
            # Cleanup
            import shutil
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
    
    def test_end_to_end_processing(self, ingestor, temp_csv_file):
        """Test complete processing pipeline"""
        temp_dir = tempfile.mkdtemp()
        output_path = os.path.join(temp_dir, "processed")
        
        try:
            df, quality_report, aggregations = ingestor.process(
                input_path=temp_csv_file,
                output_path=output_path,
                partition_by=["year", "month"]
            )
            
            # Verify processing completed
            assert df is not None
            assert quality_report is not None
            assert aggregations is not None
            
            # Verify output
            assert os.path.exists(output_path)
            assert quality_report["validity_rate"] > 0
            assert aggregations["overall"]["total_transactions"] > 0
            
        finally:
            # Cleanup
            import shutil
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)


class TestDataValidationRules:
    """
    Test specific validation rules
    """
    
    def test_bic_pattern_valid(self, ingestor):
        """Test valid BIC patterns"""
        valid_bics = [
            "BOFAUS3NXXX",
            "CHASUS33XXX",
            "DEUTDEFFXXX",
            "BARCGB22XXX",
            "BNPAFRPPXXX"
        ]
        
        for bic in valid_bics:
            import re
            assert re.match(ingestor.BIC_PATTERN, bic) is not None
    
    def test_bic_pattern_invalid(self, ingestor):
        """Test invalid BIC patterns"""
        invalid_bics = [
            "INVALID",
            "BOF",
            "BOFAUS3",
            "123456789",
            "bofaus3nxxx",  # lowercase
            "BOFAU$3NXXX",  # special char
        ]
        
        for bic in invalid_bics:
            import re
            assert re.match(ingestor.BIC_PATTERN, bic) is None
    
    def test_valid_currencies(self, ingestor):
        """Test that common currencies are in valid list"""
        assert "USD" in ingestor.VALID_CURRENCIES
        assert "EUR" in ingestor.VALID_CURRENCIES
        assert "GBP" in ingestor.VALID_CURRENCIES
        assert "JPY" in ingestor.VALID_CURRENCIES
    
    def test_valid_message_types(self, ingestor):
        """Test that SWIFT message types are valid"""
        assert "MT103" in ingestor.VALID_MESSAGE_TYPES
        assert "MT202" in ingestor.VALID_MESSAGE_TYPES
        assert "MT950" in ingestor.VALID_MESSAGE_TYPES


class TestEdgeCases:
    """
    Test edge cases and boundary conditions
    """
    
    def test_empty_dataframe(self, ingestor, spark):
        """Test processing empty DataFrame"""
        schema = ingestor.define_schema()
        empty_df = spark.createDataFrame([], schema)
        
        transformed_df = ingestor.transform_data(empty_df)
        validated_df = ingestor.validate_data(transformed_df)
        
        assert validated_df.count() == 0
        
        report = ingestor.generate_quality_report(validated_df)
        assert report["total_records"] == 0
        assert report["validity_rate"] == 0
    
    def test_single_record(self, ingestor, spark):
        """Test processing single record"""
        data = [("2024-12-01 10:30:00.000000", "BOFAUS3NXXX", "CHASUS33XXX", 50000.00, "USD", "MT103")]
        schema = ingestor.define_schema()
        df = spark.createDataFrame(data, schema)
        
        transformed_df = ingestor.transform_data(df)
        validated_df = ingestor.validate_data(transformed_df)
        
        assert validated_df.count() == 1
        assert validated_df.filter(validated_df.is_valid).count() == 1
    
    def test_large_amount_boundary(self, ingestor, spark):
        """Test amounts at high-value boundary"""
        data = [
            ("2024-12-01 10:30:00.000000", "BOFAUS3NXXX", "CHASUS33XXX", 9999999.99, "USD", "MT103"),
            ("2024-12-01 10:30:00.000000", "BOFAUS3NXXX", "CHASUS33XXX", 10000000.00, "USD", "MT103"),
            ("2024-12-01 10:30:00.000000", "BOFAUS3NXXX", "CHASUS33XXX", 10000000.01, "USD", "MT103"),
        ]
        schema = ingestor.define_schema()
        df = spark.createDataFrame(data, schema)
        
        transformed_df = ingestor.transform_data(df)
        validated_df = ingestor.validate_data(transformed_df)
        
        high_value_count = validated_df.filter(validated_df.is_high_value).count()
        assert high_value_count == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
