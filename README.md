# Real_time_Health_Alert_Notification
Inspired from the smart wearables and IOT devices and propose data pipeline which capture
high-velocity stream of patient vitals such as heartbeat, temperature, blood pressure and etc
and email notification is sent when abnormal vitals are detected.


## Table of Contents
1. A script to run/start Kafka server, create a topic(kafka.pdf)
2.	A script to create producer application to read from RDS and push the message into the topic in below format  and list the messages in the topic (kafka_produce_patient_vitals.py)
3.	A script of Pyspark application to read all messages from Kafka topic into HDFS file in parquet format in_df2.writeStream.format().. (kafka_spark_patient_vitals.py)
4.	A script to build an external hive table for the threshold data and view threshold data(hive1.pdf)
5.	A script to create hbase table with 3 families (attr, limit, msg) -- insert 12 records into hbase table(hbase.pdf)
6.	A script to create an external hive table for patients vital information and view data(hive2.pdf)
7.	A script to extract patient info using sqoop into hive table(sqoop.pdf)
8.	A script of Spark streaming application to read data from HDFS compare with hbase (kafka_spark_generate_alerts.py)
9.	A script of consumer application to send an email (kafka_consume_alerts.py)
10.	A screenshot of successful SNS configuration(sns.pdf)
11.	A document comprising your overall code logic. This document should also have the commands needed to run all the scripts mentioned above. (code_logic.pdf)
