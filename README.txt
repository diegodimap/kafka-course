download kafka: https://kafka.apache.org/downloads

go to the downloaded folder:
zookeeper-server-start.bat ..\..\config\zookeeper.properties

kafka-server-start.bat ..\..\config\server.properties

kafka-topics.bat --bootstrap-server 127.0.0.1:9092 --create --topic demo_java --partitions 3 --replication-factor 1

kafka-console-consumer.bat --bootstrap-server 127.0.0.1:9092 --topic demo_java
