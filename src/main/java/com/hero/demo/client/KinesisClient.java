package com.hero.demo.client;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;
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

    @Autowired
    private ConcurrentLinkedQueue<List<String>> records;

    @Autowired
    private IRecordProcessorFactory recordProcessorFactory;

    @Autowired
    private KinesisClientLibConfiguration kinesisClientLibConfiguration;

    @Autowired
    private KinesisProducerConfiguration kinesisProducerConfiguration;

    @PostConstruct
    private void create() {
        producer = startProducer();
    }

    private KinesisProducer startProducer() {

        return new KinesisProducer(kinesisProducerConfiguration);
    }

    public ListenableFuture<UserRecordResult> send(String message) throws ExecutionException, InterruptedException {
        ByteBuffer data = ByteBuffer.wrap(message.getBytes());
        UserRecord userRecord = new UserRecord(KINESIS_STREAM_NAME, "key", data);
        return producer.addUserRecord(userRecord);
    }

    public Collection<String> getRecords(int count) {
        List<String> values = records.stream().flatMap(Collection::stream).collect(Collectors.toList());
        int begin = values.size() - count;
        int end = values.size();

        if (begin < 0) {
            begin = 0;
        }

        return values.subList(begin, end);
    }
}
