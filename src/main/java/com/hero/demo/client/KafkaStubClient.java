package com.hero.demo.client;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by play on 10/10/18.
 */
@Component
public class KafkaStubClient {

    @Value("${kafka.bootstrap-servers}")
    private String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    @Value("${kafka.topics}")
    private static String TOPICS = "mytopic.t";

    private Stack<String> values = new Stack<String>();

    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;


    @PostConstruct
    private void create() {
        producer = startProducer();
        consumer = startConsumer();
    }

    private KafkaProducer<String, String> startProducer() {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer(producerProps);
    }

    public RecordMetadata send(String message) throws ExecutionException, InterruptedException {
        return producer.send(new ProducerRecord<String, String>(TOPICS,message)).get();
    }

    public Collection<String> getRecords(int count) {
        int begin = values.size() - count;
        int end = values.size();

        if (begin < 0) {
            begin = 0;
        }
        return values.subList(begin, end);
    }

    private KafkaConsumer<String, String> startConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaMockConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Collections.singletonList(TOPICS));

        Timer timer = new Timer();
        timer.schedule(wrap(() -> {
            consumer
                    .poll(1000)
                    .forEach(c -> {
                        values.push(c.value());
                        System.out.println(c.key() + "->" + c.value());
                    });
        }), 500, 500);

        return consumer;
    }

    static TimerTask wrap(Runnable r) {
        return new TimerTask() {

            @Override
            public void run() {
                r.run();
            }
        };
    }
}
