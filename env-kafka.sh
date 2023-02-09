docker exec -it ${container_id} /bin/bash
#create topics
kafka-topics --bootstrap-server localhost:29092 --topic first_topic --create --partitions 3 --replication-factor 1
##kafka-topics --bootstrap-server localhost:29092 --topic second-topic --create --partitions 3 --replication-factor 1
#list topics
kafka-topics --bootstrap-server localhost:29092 --list
#create a message
kafka-console-producer --bootstrap-server localhost:29092 --topic first_topic


### Examples messages
{ "firstName": "StringType", "age": 11, "bornDate": "2012-01-01"}



### execute jar

spark-submit --class com.data.factory.App .\target\kafka-to-spark-streaming-1.0-SNAPSHOT-jar-with-dependencies.jar ewogICJpbnB1dFN0cmVhbSI6IHsKICAgICJrYWZrYSI6IHsKICAgICAgInRvcGljTmFtZSI6ICJmaXJzdF90b3BpYyIsCiAgICAgICJib290U3RyYXBTZXJ2ZXIiOiAibG9jYWxob3N0OjI5MDkyIiwKICAgICAgIm1lc3NhZ2VTdHJ1Y3QiOiBbCiAgICAgICAgewogICAgICAgICAgImZpZWxkTmFtZSI6ICJmaXJzdE5hbWUiLAogICAgICAgICAgImZpZWxkVHlwZSI6ICJTdHJpbmdUeXBlIiwKICAgICAgICAgICJudWxsYWJsZSI6IHRydWUKICAgICAgICB9LAogICAgICAgIHsKICAgICAgICAgICJmaWVsZE5hbWUiOiAiYWdlIiwKICAgICAgICAgICJmaWVsZFR5cGUiOiAiSW50ZWdlclR5cGUiLAogICAgICAgICAgIm51bGxhYmxlIjogdHJ1ZQogICAgICAgIH0sCiAgICAgICAgewogICAgICAgICAgImZpZWxkTmFtZSI6ICJib3JuRGF0ZSIsCiAgICAgICAgICAiZmllbGRUeXBlIjogIkRhdGVUeXBlIiwKICAgICAgICAgICJudWxsYWJsZSI6IHRydWUKICAgICAgICB9CiAgICAgIF0KICAgIH0KICB9LAogICJvdXRwdXRTdHJlYW0iOiB7CiAgICAiY3N2IjogewogICAgICAicGF0aCI6ICJDOlxcVXNlcnNcXGx1YmFyXFxPbmVEcml2ZVxcRG9jdW1lbnRvc1xcR2l0SHViXFxrYWZrYS10by1zcGFyay1zdHJlYW1pbmciLAogICAgICAiZGVsaW1pdGVyIjogIiwiLAogICAgICAiaGVhZGVyIjogdHJ1ZQogICAgfQogIH0KfQ