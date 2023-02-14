# Kafka consumer with spark streaming
This service solve the use case when you need to read a json message from kafka topic and you need to write those messages into a file. Therefore, this service allows read a kafka topic using spark streaming as consumer.  also, this service allows define json schema for your message. Finally, this service allows to write the messages to multiples files such as parquet or csv.

## Pre requirements
 You need to have installed the followings tools:
- `docker` and `docker compose`
- `mvn`
- `spark`

## Payload
When you run your application, you need to define your payload. This must be Base64 Encode and it must have the following fields:
```json
{
  "inputStream": {
    "kafka": {
      "topicName": "first_topic",
      "bootStrapServer": "localhost:29092",
      "messageStruct": [
        {
          "fieldName": "firstName",
          "fieldType": "StringType",
          "nullable": true
        },
        {
          "fieldName": "age",
          "fieldType": "IntegerType",
          "nullable": true
        },
        {
          "fieldName": "bornDate",
          "fieldType": "DateType",
          "nullable": true
        }
      ]
    }
  },
  "outputStream": {
    "csv": {
      "path": "output/",
      "delimiter": ",",
      "header": true
    }
  }
}
```

## How to use?
You need to create a kafka environment.
```bash
docker compose up
```
Also, you need to create a sample kafka topic. Therefore, you need to enter to the kafka container with the following commands.
```bash
# Get kafka container_id
container_id=$(docker ps | grep "kafka" | awk '{print $1}')

# Enter to container
docker exec -it ${container_id} /bin/bash

# Create a topic named my-topic
kafka-topics --bootstrap-server localhost:29092 --topic my-topic --create --partitions 1 --replication-factor 1

# Validate topic created
kafka-topics --bootstrap-server localhost:29092 --list

# Finally yo can publish message into topic
kafka-console-producer --bootstrap-server localhost:29092 --topic my-topic

# Message Example
{"firstName": "luis", "age": 11, "bornDate": "2012-01-01"} 
```

## Kafka message example
```json
{
  "firstName": "luis", 
  "age": 11, 
  "bornDate": "2012-01-01"
}

```
Finally, you must compile the jar and run the application.
```bash
mvn clean package
spark-submit --class com.data.factory.App target/kafka-to-spark-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar ewogICJpbnB1dFN0cmVhbSI6IHsKICAgICJrYWZrYSI6IHsKICAgICAgInRvcGljTmFtZSI6ICJteS10b3BpYyIsCiAgICAgICJib290U3RyYXBTZXJ2ZXIiOiAibG9jYWxob3N0OjI5MDkyIiwKICAgICAgIm1lc3NhZ2VTdHJ1Y3QiOiBbCiAgICAgICAgewogICAgICAgICAgImZpZWxkTmFtZSI6ICJmaXJzdE5hbWUiLAogICAgICAgICAgImZpZWxkVHlwZSI6ICJTdHJpbmdUeXBlIiwKICAgICAgICAgICJudWxsYWJsZSI6IHRydWUKICAgICAgICB9LAogICAgICAgIHsKICAgICAgICAgICJmaWVsZE5hbWUiOiAiYWdlIiwKICAgICAgICAgICJmaWVsZFR5cGUiOiAiSW50ZWdlclR5cGUiLAogICAgICAgICAgIm51bGxhYmxlIjogdHJ1ZQogICAgICAgIH0sCiAgICAgICAgewogICAgICAgICAgImZpZWxkTmFtZSI6ICJib3JuRGF0ZSIsCiAgICAgICAgICAiZmllbGRUeXBlIjogIkRhdGVUeXBlIiwKICAgICAgICAgICJudWxsYWJsZSI6IHRydWUKICAgICAgICB9CiAgICAgIF0KICAgIH0KICB9LAogICJvdXRwdXRTdHJlYW0iOiB7CiAgICAiY3N2IjogewogICAgICAicGF0aCI6ICJvdXRwdXQvIiwKICAgICAgImRlbGltaXRlciI6ICIsIiwKICAgICAgImhlYWRlciI6IHRydWUKICAgIH0KICB9Cn0=
```

