# kafka-file-connector
kafka filestream sink connector

## Log Format
- Rotated and stored in desinated MB
- Examples  
${PROJECT_NAME}/${PODNAME}.log  
${PROJECT_NAME}/${PODNAME}-YYYYMMDD_1.log  
${PROJECT_NAME}/${PODNAME}-YYYYMMDD_2.log  

## Sink Connector Config
```
{
    "class": "org.gd.connect.FileStreamSinkConnector",
    "type": "sink",
    "version": "2.8.2"
}
```

## Connector Json Example
```
{  
"name": "file-sink-ts",  
"config": {
  "topics":"dept.human.topic",
   "connector.class":"org.gd.connect.FileStreamSinkConnector",
	 "tasks.max":1,
	 "file":"/home/kafka/connect/dept/human/{filename}.log",
   "max.size": 10485760
}
```
