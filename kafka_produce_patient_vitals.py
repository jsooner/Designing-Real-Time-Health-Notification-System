# Import required dependencies
from kafka import KafkaProducer
import mysql.connector
import time
import json

# Collect whole data from rds
connection = mysql.connector.connect(host='upgraddetest.cyaielc9bmnf.us-east-1.rds.amazonaws.com', database='testdatabase',user='student', password='STUDENT123')
cursor = connection.cursor()
query = "SELECT customerId, heartBeat,bp FROM patients_vital_info"

# Execute the above query
cursor.execute(query)
patient_vital_records = cursor.fetchall()

cursor.close()
connection.close()

# Setting up the producer configurations
topicName = 'patient_vital_topic'
producer = KafkaProducer(bootstrap_servers = ['ec2-44-196-94-216.compute-1.amazonaws.com:9092'],value_serializer=lambda x:json.dumps(x).encode('utf-8'))

# Sending the message from the producer every second
for customerId,heartBeat,bp in patient_vital_records:
    
    # create a message to send in JSON format
    msg = {"customerId":customerId,"heartBeat":heartBeat,"bp":bp}

    # send the message to the Kafka topic
    producer.send(topicName,msg)
    print(msg)
    
    # wait for one second before sending the next message
    time.sleep(1)