import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit 

def profile_fields(input_file, fields):
    # Create a Spark session with the application name "Field Profiler"
    spark = SparkSession.builder.appName("Field Profiler").getOrCreate()
    
    try:
        # Read the CSV input file into a DataFrame, inferring schema and including headers
        df = spark.read.csv(input_file, header=True, inferSchema=True)
    except Exception as e:
        # Print an error message if file reading fails and exit the program
        print(f"Error reading file: {e}")
        sys.exit(1)

    # Get the list of available fields (columns) in the DataFrame
    available_fields = df.columns
    
    # Validate that the requested fields exist in the DataFrame
    for field in fields:
        if field not in available_fields:
            # Print an error message if a field is not found and exit the program
            print(f"Field '{field}' not found in the dataset.")
            sys.exit(1)
    
    results = {}  # Initialize a dictionary to store profiling results
    for field in fields:
        # Group the DataFrame by the specified field, count occurrences, sort by count, and limit to the top 500
        field_df = (
            df.groupBy(field)
            .count()
            .orderBy("count", ascending=False)
            .limit(500)
        )
        # Collect the results from the grouped DataFrame into a list
        field_data = field_df.collect()
        # Populate the results dictionary with field values and their corresponding counts
        results[field] = {row[field]: row["count"] for row in field_data}

    # Stop the Spark session to free up resources
    spark.stop()

    return results  # Return the profiling results as a dictionary

if __name__ == "__main__":
    # Check if the script is run with the correct number of command line arguments
    if len(sys.argv) < 3:
        print("Usage: python profile_fields.py <input_file> <fields>")
        sys.exit(1)

    # Get the input file path and fields from command line arguments
    input_file = sys.argv[1]
    fields = sys.argv[2].split(",")  # Split the fields by comma into a list

    try:
        # Call the profiling function and store the results
        profile_results = profile_fields(input_file, fields)
        # Print the results in a formatted JSON output
        print(json.dumps(profile_results, indent=4))
        sys.exit(0)  # Exit the program successfully
    except Exception as e:
        # Print an error message if any exceptions occur and exit with a failure code
        print(f"An error occurred: {e}")
        sys.exit(1)
