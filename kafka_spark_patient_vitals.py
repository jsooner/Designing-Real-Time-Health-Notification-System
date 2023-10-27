# Import the required dependencies
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder.appName('kafka_spark_patient_vitals').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Reading data from Kafka topic
vital_info = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","ec2-44-196-94-216.compute-1.amazonaws.com:9092")  \
	.option("subscribe","patient_vital_topic")  \
	.option("startingOffsets", "earliest")  \
	.load()

# Define custom schema for patient vitals
patient_vital_schema = StructType([StructField('customerId', IntegerType(), True),
                    StructField('heartBeat', IntegerType(), True),
                    StructField('bp', IntegerType(), True)])

# Create the Spark dataframe by parsing JSON as per the schema 
vital_df = vital_info.select(from_json(col('value').cast('string'),patient_vital_schema).alias('data')).select('data.*')

# Add timestamp column to the patient vitals
vital_timestamp_df = vital_df.withColumn('Message_time',current_timestamp())  \
	.withColumnRenamed('customerId','CustomerID')  \
	.withColumnRenamed('bp','BP')  \
	.withColumnRenamed('heartBeat','HeartBeat')  \
	.withColumn('date',lit('2022-03-16'))  \

# Print schema of a single patient vitals
vital_timestamp_df.printSchema()

final_df = vital_timestamp_df.select('CustomerID','BP', 'HeartBeat','Message_time','date')

# Write the patient vital info to HDFS in parquet format
final_df.writeStream \
   .partitionBy("date") \
   .outputMode("append") \
   .option("truncate", "false") \
   .option("path", "/user/livy/output/") \
   .option("checkpointLocation", "/tmp/checkpoint/") \
   .start() \
   .awaitTermination()
