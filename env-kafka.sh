docker exec -it ${container_id} /bin/bash
#create topics
kafka-topics --bootstrap-server localhost:29092 --topic first-topic --create --partitions 3 --replication-factor 1
##kafka-topics --bootstrap-server localhost:29092 --topic second-topic --create --partitions 3 --replication-factor 1
#list topics
kafka-topics --bootstrap-server localhost:29092 --list
#create a message
kafka-console-producer.sh --bootstrap-server localhost:29092 --topic first_topic


### Examples messages
{"x": "1", "y": "one"}
{"x": "2", "y": "two"}
{"x": "3", "y": "three"}



### execute jar

