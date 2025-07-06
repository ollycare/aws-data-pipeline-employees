import sys
from pyspark.sql.functions import col, expr
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Initialize GlueContext and SparkContext
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read the CSV file from the S3 bucket using AWS Glue Data Catalog
datasource = glueContext.create_dynamic_frame.from_catalog(
    database = "company", 
    table_name = "year_2024", 
    transformation_ctx = "datasource"
)

# Convert the Glue DynamicFrame to a Spark DataFrame for data manipulation
df = datasource.toDF()

# Increment Salary by 10%
df = df.withColumn("Salary", col("Salary") * 1.10)

# Write the updated DataFrame back to S3 in CSV format
df.write.csv('s3://data-pipeline-employees/company/Year-2025/employees-metadata.csv', header=True)

# Commit the Glue job
job.commit()
