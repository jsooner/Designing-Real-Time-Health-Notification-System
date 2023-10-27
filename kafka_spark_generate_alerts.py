from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ReadHiveTable").enableHiveSupport().getOrCreate()

# Patients contact Information table
patient_contact_table = "patient_health_care.Patients_Contact_Info"
patient_contact_df = spark.table(patient_contact_table)

# Define the schema for the streaming data
schema = StructType([StructField('CustomerID', IntegerType(), True),
                    StructField('BP', IntegerType(), True),
                    StructField('HeartBeat', IntegerType(), True),
                    StructField('Message_time', TimestampType(), True)])

# Read patient vitals streaming data from an HDFS location
patient_vital_df = spark.readStream.format("parquet") \
    .schema(schema) \
    .load("/user/livy/output/date=2022-03-16")

# Creating a temporary table for streaming data
patient_vital_df.createOrReplaceTempView("Patients_Vital_Info")

# Selecting the patient details with abnormal vitals
alert_df = spark.sql("SELECT patientname, age, patientaddress, phone_number, admitted_ward, BP as bp, HeartBeat as heartBeat, Message_time as input_Message_time, alert_message FROM Patients_Vital_Info v, patient_health_care.Threshold_Reference T, patient_health_care.Patients_Contact_Info C WHERE ((C.patientid = v.CustomerID) AND (C.age BETWEEN T.low_age_limit AND T.high_age_limit) AND ((T.attribute = 'bp' AND (v.BP BETWEEN T.low_range_value AND T.high_range_value)) OR (T.attribute = 'heartBeat' AND (v.HeartBeat BETWEEN T.low_range_value AND T.high_range_value))) AND T.alert_flag == 1)")

# Generating a new column with current timestamp
alert_df = alert_df.withColumn("alert_generated_time", current_timestamp())

# Write streaming health alerts data to Alerts_Message kafka topic
alert_df.selectExpr( "to_json(struct(*)) AS value")\
            .writeStream\
            .format("kafka")\
            .outputMode("append")\
            .option("kafka.bootstrap.servers","ec2-44-196-94-216.compute-1.amazonaws.com:9092")  \
            .option("topic", "Alerts_Message")\
            .option("checkpointLocation", "/tmp/alert_checkpoint/") \
            .start()\
            .awaitTermination()
