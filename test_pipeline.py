#!/usr/bin/env python3
"""
Simple test script to validate the data pipeline functionality.

This script runs basic tests on the pipeline components to ensure
everything is working correctly before deployment.
"""

import sys
import os
from pathlib import Path

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Import once at module level
from data_pipeline import parse_price_spark, apply_filters, get_sample_output
from spark_utils import create_spark_session


def test_price_parsing():
    """Test price parsing functionality with PySpark."""
    print("Testing price parsing...")
    
    try:
        spark = create_spark_session("TestPriceParsing")
        try:
            # Create test DataFrame
            test_data = [
                {"id": "test1", "raw_price": "530 000€/mo.", "living_area": 100.0, 
                 "property_type": "apartment", "municipality": "Test", "scraping_date": "2021-01-01"},
                {"id": "test2", "raw_price": "1 573 000€/mo.", "living_area": 100.0,
                 "property_type": "apartment", "municipality": "Test", "scraping_date": "2021-01-01"},
                {"id": "test3", "raw_price": "550 000€/mo.", "living_area": 100.0,
                 "property_type": "apartment", "municipality": "Test", "scraping_date": "2021-01-01"},
            ]
            
            df = spark.createDataFrame(test_data)
            df_with_price = parse_price_spark(df)
            
            # Collect results for validation
            results = df_with_price.select("raw_price", "price").collect()
            
            expected_results = [
                ("530 000€/mo.", 530000.0),
                ("1 573 000€/mo.", 1573000.0),
                ("550 000€/mo.", 550000.0),
            ]
            
            for i, (raw_price, expected_price) in enumerate(expected_results):
                actual_price = results[i]['price']
                if actual_price == expected_price:
                    print(f"  ✓ '{raw_price}' -> {actual_price}")
                else:
                    print(f"  ✗ '{raw_price}' -> {actual_price}, expected {expected_price}")
                    return False
            
            return True
        finally:
            spark.stop()
    except Exception as e:
        print(f"  ✗ Error in price parsing test: {e}")
        return False


def test_filtering():
    """Test filtering logic with PySpark."""
    print("Testing filtering logic...")
    
    try:
        spark = create_spark_session("TestFiltering")
        try:
            # Create test data with various filter conditions
            test_data = [
                # Valid record
                {"id": "valid", "scraping_date": "2021-06-15", "property_type": "apartment",
                 "municipality": "Test", "price": 500000.0, "living_area": 100.0, "price_per_square_meter": 5000.0},
                # Invalid property type
                {"id": "invalid_type", "scraping_date": "2021-06-15", "property_type": "studio",
                 "municipality": "Test", "price": 500000.0, "living_area": 100.0, "price_per_square_meter": 5000.0},
                # Invalid price per sqm (too high)
                {"id": "invalid_price", "scraping_date": "2021-06-15", "property_type": "apartment",
                 "municipality": "Test", "price": 2000000.0, "living_area": 100.0, "price_per_square_meter": 20000.0},
                # Invalid date (too old)
                {"id": "invalid_date", "scraping_date": "2020-01-01", "property_type": "apartment",
                 "municipality": "Test", "price": 500000.0, "living_area": 100.0, "price_per_square_meter": 5000.0},
            ]
            
            from pyspark.sql.types import StructType, StructField, StringType, DoubleType
            
            schema = StructType([
                StructField("id", StringType(), False),
                StructField("scraping_date", StringType(), False),
                StructField("property_type", StringType(), False),
                StructField("municipality", StringType(), False),
                StructField("price", DoubleType(), False),
                StructField("living_area", DoubleType(), False),
                StructField("price_per_square_meter", DoubleType(), False)
            ])
            
            df = spark.createDataFrame(test_data, schema)
            
            # Convert string dates to proper dates for filtering
            from pyspark.sql import functions as func
            df = df.withColumn("scraping_date", func.to_date(func.col("scraping_date"), "yyyy-MM-dd"))
            
            # Apply filters
            filtered_df = apply_filters(df)
            
            # Should only have 1 valid record
            result_count = filtered_df.count()
            if result_count == 1:
                print("  ✓ Filtering correctly removed invalid records")
                
                # Check that the valid record remains
                valid_record = filtered_df.filter(func.col("id") == "valid").collect()
                if len(valid_record) == 1:
                    print("  ✓ Valid record passed all filters")
                    return True
                else:
                    print("  ✗ Valid record was incorrectly filtered out")
                    return False
            else:
                print(f"  ✗ Expected 1 record after filtering, got {result_count}")
                return False
        finally:
            spark.stop()
    except Exception as e:
        print(f"  ✗ Error in filtering test: {e}")
        return False


def test_pipeline_integration():
    """Test the complete pipeline with sample data using PySpark."""
    print("Testing pipeline integration...")
    
    input_file = "input/scraping_data.jsonl"
    
    if not Path(input_file).exists():
        print(f"  ✗ Input file not found: {input_file}")
        return False
    
    try:
        spark = create_spark_session("TestIntegration")
        try:
            stats = run_pipeline(spark, input_file, ":memory:")
            
            print(f"  ✓ PySpark pipeline completed successfully")
            print(f"    - Input records: {stats['total_input_records']}")
            print(f"    - Failed processing: {stats['failed_processing']}")
            print(f"    - Filtered out: {stats['filtered_out']}")
            print(f"    - Final records: {stats['final_records']}")
            
            if stats['final_records'] == 0:
                print("  ✗ No records in final output")
                return False
            
            # Get sample output
            import duckdb
            duckdb_conn = duckdb.connect(":memory:")
            try:
                samples = get_sample_output(duckdb_conn, 2)
                print(f"  ✓ Sample output: {len(samples)} records")
            finally:
                duckdb_conn.close()
            
            for sample in samples:
                print(f"    - {sample}")
            
            # Print Spark UI info
            if spark.sparkContext.uiWebUrl:
                print(f"  ✓ Spark UI was available at: {spark.sparkContext.uiWebUrl}")
            
            return True
        finally:
            spark.stop()
    except Exception as e:
        print(f"  ✗ PySpark pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("=" * 50)
    print("PySpark Property Data Pipeline Test Suite")
    print("=" * 50)
    
    tests = [
        ("Price Parsing", test_price_parsing),
        ("Filtering Logic", test_filtering),
        ("Pipeline Integration", test_pipeline_integration),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        print("-" * 30)
        
        try:
            if test_func():
                print(f"✅ {test_name} PASSED")
                passed += 1
            else:
                print(f"❌ {test_name} FAILED")
        except Exception as e:
            print(f"❌ {test_name} ERROR: {e}")
    
    print("\n" + "=" * 50)
    print(f"Test Results: {passed}/{total} tests passed")
    print("=" * 50)
    
    if passed == total:
        print("🎉 All tests passed! PySpark pipeline is ready for deployment.")
        return 0
    else:
        print("⚠️  Some tests failed. Please review the issues above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
