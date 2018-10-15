package com.hero.demo.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class RecordProcessor implements IRecordProcessor {

    private Logger logger = LoggerFactory.getLogger(RecordProcessor.class);

    private String kinesisShardId;

    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;
    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

    private ConcurrentLinkedQueue<List<String>> processedRecords;

    private Integer batchSize;

    public RecordProcessor(ConcurrentLinkedQueue<List<String>> processedRecords,
                           Integer batchSize) {
        this.processedRecords = processedRecords;
        this.batchSize = batchSize;
    }


    @Override
    public void initialize(String shardId) {
        logger.info("Initializing record processor for shard: " + shardId);

        this.kinesisShardId = shardId;
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        logger.info("Processing " + records.size() + " records from " + kinesisShardId);

        // Process records and perform all exception handling.
        List<Record> batch = records.stream().limit(50).collect(Collectors.toList());
        int processedRecordCount = 0;
        while (batch.size() > 0) {
            processedRecordCount += batchSize;
            processRecordsWithRetries(batch);
            batch = records.stream().skip(processedRecordCount).limit(batchSize).collect(Collectors.toList());
        }

        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
    }

    private void processRecordsWithRetries(List<Record> records) {
        boolean processedSuccessfully = false;
        Integer totalRecordCount = records.size();

        logger.debug("Processing {} records with retries.", totalRecordCount);

        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                List<String> data =
                        records.stream()
                                .map(record -> new String(record.getData().array(), StandardCharsets.UTF_8)).collect(Collectors.toList());
                appendProcessedRecords(data);

                processedSuccessfully = true;
                break;
            } catch (Throwable t) {
                logger.warn("Caught throwable while processing record " + records, t);
            }

            // backoff if we encounter an exception.
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted sleep", e);
            }
        }

        if (!processedSuccessfully) {
            logger.error("Couldn't process record " + records + ". Skipping the record.");
        }
    }

    private void appendProcessedRecords(List<String> userRecords) {
        if (processedRecords != null) {
            logger.info("Appending userRecords, count={}", userRecords == null ? 0 : userRecords.size());
            processedRecords.add(userRecords);
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        logger.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        logger.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                logger.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                    logger.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    logger.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + NUM_RETRIES, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                logger.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted sleep", e);
            }
        }
    }
}
