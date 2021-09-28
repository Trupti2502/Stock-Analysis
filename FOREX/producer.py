#This file is going to be used for producer 
#first create a topic name as 'project3'
from kafka import KafkaProducer
import requests
from json import dumps
import time

kafka_data_producers = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8') )

while True:
    response_data = requests.get("https://api.tiingo.com/tiingo/fx/top?tickers=audusd,eurusd&token=<PASS UR API KEY HERE> ")
    #response_data=response_data.json()
    
    data = {'Lagos' : response_data.json()}
    data=data['Lagos'][0]
    kafka_data_producers.send('project3', value=data)
    print(data)
    print()
    time.sleep(10)
