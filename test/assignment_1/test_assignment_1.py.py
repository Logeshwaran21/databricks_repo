# Databricks notebook source
# MAGIC %run /Users/logeshwaran.a@diggibyte.com/assignment_1/source_to_bronze/util

# COMMAND ----------

import logging
from pyspark.sql import SparkSession


# Create a SparkSession
spark = SparkSession.builder .appName("Utils Test").getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)



def test_read_csv_with_custom_schema():
    # Test with a sample schema and file
    schema = "EmployeeID INT, EmployeeName STRING, Department STRING, Country STRING, Salary INT, Age INT"
    file_path = "/FileStore/src/assignment_1/resources/test_employee.csv"
    df = read_csv_with_custom_schema(file_path, schem)
    
    # Perform assertions
    assert df.count() > 0, "DataFrame is empty"
    logging.info("read_csv_with_custom_schema test passed")


def test_convert_camel_to_snake_case():
    # Create a sample DataFrame
    data = [(1, "Sathya", "IT"), (2, "Eniyan", "HR")]
    columns = ["EmployeeID", "EmployeeName", "Department"]
    df = spark.createDataFrame(data, columns)
    
    # Apply conversion
    df_snake_case = convert_camel_to_snake_case(df)
    
    # Perform assertions
    expected_columns = ["employeeid", "employeename", "department"]
    assert df_snake_case.columns == expected_columns
    logging.info("convert_camel_to_snake_case test passed")

def test_write_delta_table():
    # Create a sample DataFrame
    data = [(1, "Sathya Priya", "IT", "USA", 50000, 30)]
    columns = ["EmployeeID", "EmployeeName", "Department", "Country", "Salary", "Age"]
    df = spark.createDataFrame(data, columns)
    
    # Write DataFrame as Delta table
    database = "test_db"
    table = "test_table"
    primary_key = "EmployeeID"
    path = "/FileStore/src/assignment_1/resources/test_delta_table"
    write_delta_table(df, database, table, primary_key, path)
    logging.info("write_delta_table test passed")

# Run the tests
test_read_csv_with_custom_schema()
test_convert_camel_to_snake_case()
test_write_delta_table()

# COMMAND ----------

