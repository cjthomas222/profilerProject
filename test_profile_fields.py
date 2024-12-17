import os
import shutil
from pyspark.sql import SparkSession
from script import profile_fields


def test_profile_fields():
    # Setup Spark session
    spark = SparkSession.builder \
        .appName("TestFieldProfiler") \
        .master("local[*]") \
        .getOrCreate()

    # Create sample data
    test_data = [("John", "Doe", "M"),
                 ("Jane", "Smith", "F"),
                 ("Alex", "Johnson", "U")]
    schema = ["First Name", "Last Name", "Gender"]
    df = spark.createDataFrame(test_data, schema=schema)

    # Save as CSV for testing
    test_csv = "test_data"
    df.write.csv(test_csv, header=True, mode="overwrite")

    # Run profiling function
    fields = ["Gender"]
    result = profile_fields(test_csv, fields)

    # Assertions
    assert "Gender" in result
    assert result["Gender"]["M"] == 1
    assert result["Gender"]["F"] == 1
    assert result["Gender"]["U"] == 1

    # Cleanup the output directory
    shutil.rmtree(test_csv)

    spark.stop()
