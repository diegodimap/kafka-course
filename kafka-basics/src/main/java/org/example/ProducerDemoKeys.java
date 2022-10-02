package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Testing!");


        //create producer configs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //same keys go to same partition
        for (int i = 1; i <= 10 ; i++) {

            String topic = "demo_java";
            String value = "testing java" + i;
            String key = "id_" + i;

            //create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            //send data async
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    //executes every time a record is successfully sent or an exception is thrown
                    if(e==null){
                        log.info("received metadata! ");
                        log.info("topic: " + metadata.topic());
                        log.info("key: " + producerRecord.key());
                        log.info("partition: " + metadata.partition());
                        log.info("offset: " + metadata.offset());
                        log.info("timestamp: " + metadata.timestamp());
                    }else{
                        log.error("Error while producing", e);
                    }
                }
            });
        }

        //flush and close the producer
        producer.flush();
        producer.close();

    }

    /*
        download kafka: https://kafka.apache.org/downloads

        go to the downloaded folder:
        zookeeper-server-start.bat ..\..\config\zookeeper.properties

        kafka-server-start.bat ..\..\config\server.properties

        kafka-topics.bat --bootstrap-server 127.0.0.1:9092 --create --topic demo_java --partitions 3 --replication-factor 1

        kafka-console-consumer.bat --bootstrap-server 127.0.0.1:9092 --topic demo_java

     */

}
