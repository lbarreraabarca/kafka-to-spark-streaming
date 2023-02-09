# kafka-to-spark-streaming



## Payload
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
      "path": "C:\\Users\\lubar\\OneDrive\\Documentos\\GitHub\\kafka-to-spark-streaming",
      "delimiter": ",",
      "header": true
    }
  }
}
```