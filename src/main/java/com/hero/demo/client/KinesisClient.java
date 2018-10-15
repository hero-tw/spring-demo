package com.hero.demo.client;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;
import com.hero.demo.serializers.AvroSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.hero.demo.configuration.KinesisConfiguration.KINESIS_STREAM_NAME;

@Component
public class KinesisClient {

    private KinesisProducer producer;

    private AvroSerializer avroSerializer;

    @Autowired
    private ConcurrentLinkedQueue<List<ByteBuffer>> records;

    @Autowired
    private KinesisProducerConfiguration kinesisProducerConfiguration;

    @PostConstruct
    private void create() {
        avroSerializer = new AvroSerializer();
        producer = startProducer();
    }

    private KinesisProducer startProducer() {

        return new KinesisProducer(kinesisProducerConfiguration);
    }

    public ListenableFuture<UserRecordResult> sendUser(Integer id, String name, String company) {
        HashMap<String, Object> userData = new HashMap<>();
        userData.put("id", id);
        userData.put("name", name);
        userData.put("company", company);

        ByteBuffer data = avroSerializer.getGenericRecord(userData, "user");

        UserRecord userRecord = new UserRecord(KINESIS_STREAM_NAME, "key", data);

        return producer.addUserRecord(userRecord);
    }

    public Collection<Map<String, Object>> getUsers(int count) {
        List<ByteBuffer> values = records.stream().flatMap(Collection::stream).collect(Collectors.toList());
        int begin = values.size() - count;
        int end = values.size();

        if (begin < 0) {
            begin = 0;
        }

        return values.subList(begin, end)
                .stream()
                .map(e -> avroSerializer.getMap(e, "user", Arrays.asList("id", "name", "company")))
                .collect(Collectors.toList());
    }
}
