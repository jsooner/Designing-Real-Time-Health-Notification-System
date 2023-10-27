# Import necessary libraries
import boto3
import json
from kafka import KafkaConsumer

# Create a kafka consumer
consumer = KafkaConsumer('Alerts_Message', bootstrap_servers = ['ec2-44-196-94-216.compute-1.amazonaws.com:9092'] , auto_offset_reset = 'earliest', value_deserializer=lambda m: m.decode('utf-8'))

# Create an SNS client using boto
sns_client = boto3.client('sns',region_name='us-east-1', aws_access_key_id = "AKQYAIR5HNQKK7OTJP76" , aws_secret_access_key = "8MtYWRpfU/RIVAhY7hX0gzjjA06JbtG2lSqf6Gk0")

sns_topic_arn = 'arn:aws:sns:us-east-1:545120555452:Health_Alerts'

# Publish messages to SNS topic
for message in consumer:
    message_obj = json.loads(message.value)
    subject_header = 'Health Alert: Patient- ' + message_obj['patientname']
    result = sns_client.publish(TopicArn = sns_topic_arn, Message=  message_obj,  Subject = subject_header )
    print(result)
