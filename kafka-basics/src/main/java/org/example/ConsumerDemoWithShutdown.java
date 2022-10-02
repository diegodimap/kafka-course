package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Consumer Demo");

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-consumer-v2";
        String topic = "demo_java";

        //create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected shutdown.");
                consumer.wakeup();

                //join the main thread
                try{
                    mainThread.join();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });

        try {
            //subscribe consumer to topics
            consumer.subscribe(Collections.singletonList(topic)); //Arrays.asList(topic) <- for multiple topics

            //poll for new data
            while (true) {
                log.info("polling");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); //get all records for 100ms

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key());
                    log.info("Value: " + record.value());
                    log.info("Partition: " + record.partition());
                    log.info("Offset: " + record.offset());
                }
            }
        }catch (WakeupException e){
            log.info("Wakeup Exception"); //expected exception when closing a consumer
        }catch (Exception e){
            log.error("Unexpected exception");
        }finally {
            consumer.close();
            log.info("Consumer is now closed!");
        }
    }
}
