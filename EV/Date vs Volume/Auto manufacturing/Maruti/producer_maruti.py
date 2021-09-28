
from kafka import KafkaProducer
import requests
from json import dumps
import time

def on_message1(message):
    producer1.send('maruti', message)
    producer1.flush()

producer1 = KafkaProducer(value_serializer=lambda m: dumps(m).encode("utf-8"), bootstrap_servers=['localhost:9092'])


# url for collecting Maruti company share data
url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=MARUTI.BSE&apikey=3T6PVO8R5LP78CHT'
r = requests.get(url)
data=r.json()

#preprocessing
del(data['Meta Data'])
df=data['Time Series (Daily)']
jsonFile={}
j=0
for i in df.keys():
    jsonFile[j]=df[i]
    jsonFile[j]['0. date']=i
    j+=1

for i in jsonFile.keys():
	jsonFile[i]["open"]=jsonFile[i]["1. open"]
	jsonFile[i]["high"]=jsonFile[i]["2. high"]
	jsonFile[i]["low"]=jsonFile[i]["3. low"]
	jsonFile[i]["close"]=jsonFile[i]["4. close"]
	jsonFile[i]["volume"]=jsonFile[i]["5. volume"]
	jsonFile[i]["date"]=jsonFile[i]["0. date"]
	del jsonFile[i]["1. open"]
	del jsonFile[i]["2. high"]
	del jsonFile[i]["3. low"]
	del jsonFile[i]["4. close"]
	del jsonFile[i]["5. volume"]
	del jsonFile[i]["0. date"]
	
	
for i in jsonFile.keys():
	on_message1(jsonFile[i])
	time.sleep(1)


