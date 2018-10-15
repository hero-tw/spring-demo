package com.hero.demo.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class RecordProcessorFactory implements IRecordProcessorFactory {

    private ConcurrentLinkedQueue<List<ByteBuffer>> processedRecordLists;

    @Value("${kinesis.records-limit}")
    private Integer batchSize;

    @Autowired
    public RecordProcessorFactory(ConcurrentLinkedQueue<List<ByteBuffer>> processedRecordLists) {
        this.processedRecordLists = processedRecordLists;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new RecordProcessor(processedRecordLists, batchSize);
    }
}