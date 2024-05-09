# Databricks notebook source

# Databricks notebook source file
# Define common functions
def read_csv_with_custom_schema(file_path, schema):
   
    spark = SparkSession.builder.getOrCreate()
    custom_schema = StructType.fromDDL(schema)
    df = spark.read.csv(file_path, header=True, schema=custom_schema)
    return df


# user defined function for camel case to snake case

def convert_camel_to_snake_case(df):
    snake_case_columns = [col(col_name).alias(col_name.lower()) for col_name in df.columns]
    return df.select(*snake_case_columns)

# user defined function for writing a delta table

def write_delta_table(df,database,table, primary_key, path):
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{database}.{table}")


# COMMAND ----------

