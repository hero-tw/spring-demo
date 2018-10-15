package com.hero.demo.configuration;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.hero.demo.ApplicationContextAware;
import com.hero.demo.kinesis.RecordProcessorFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

@Configuration
@ComponentScan("com.hero.demo")
@EnableAutoConfiguration
@PropertySource({"classpath:application.yml"})
public class KinesisConfiguration {

    private static ConcurrentLinkedQueue<List<String>> processedRecordLists;

    private static String KINESIS_REGION;

    @Value("${kinesis.region}")
    public void setKinesisRegion(String region) {
        KINESIS_REGION = region;
    }

    private static String AWS_ACCESS_KEY;

    @Value("${aws.access-key}")
    public void setAwsAccessKey(String awsAccessKey) {
        AWS_ACCESS_KEY = awsAccessKey;
    }

    private static String AWS_SECRET_KEY;

    @Value("${aws.secret-key}")
    public void setAwsSecretKey(String awsSecretKey) {
        AWS_SECRET_KEY = awsSecretKey;
    }

    public static int KINESIS_RECORDS_LIMIT;

    @Value("${kinesis.records-limit:50}")
    public void setKinesisRecordsLimit(int kinesisRecordsLimit) {
        KINESIS_RECORDS_LIMIT = kinesisRecordsLimit;
    }

    public static String KINESIS_STREAM_NAME;

    @Value("${kinesis.stream:TEST_STREAM}")
    public void setKinesisStreamName(String kinesisStreamName) {
        KINESIS_STREAM_NAME = kinesisStreamName;
    }

    public static final String APPLICATION_NAME = "HERO_DEMO";

    private static final InitialPositionInStream SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM =
            InitialPositionInStream.LATEST;

    public static String WORKER_ID;

    static {
        try {
            WORKER_ID = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Bean
    public Stack<String> getKinesisValues() {
      return new Stack<>();
    }

    @Bean
    public AWSCredentialsProvider getStaticCredentialsProvider() {
        return new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                AWS_ACCESS_KEY, AWS_SECRET_KEY));
    }

    @Bean
    @Primary
    public KinesisClientLibConfiguration getKinesisClientLibConfiguration(AWSCredentialsProvider awsCredentialsProvider) {
        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(APPLICATION_NAME,
                        KINESIS_STREAM_NAME,
                        awsCredentialsProvider,
                        WORKER_ID);
        return kinesisClientLibConfiguration.withInitialPositionInStream(SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM);
    }

    @Bean
    public KinesisProducerConfiguration getKinesisProducerConfiguration(AWSCredentialsProvider awsCredentialsProvider) {
        KinesisProducerConfiguration kinesisProducerConfiguration = new KinesisProducerConfiguration();
        kinesisProducerConfiguration.setCredentialsProvider(awsCredentialsProvider);
        kinesisProducerConfiguration.setRegion(KINESIS_REGION);
        return kinesisProducerConfiguration;
    }

    @Bean
    public static ConcurrentLinkedQueue<List<String>> processedRecordLists() {
        if (processedRecordLists == null) {
            processedRecordLists = new ConcurrentLinkedQueue<>();
        }
        return processedRecordLists;
    }

    @Bean
    public Worker getWorker(IRecordProcessorFactory recordProcessorFactory, KinesisClientLibConfiguration kinesisClientLibConfiguration) {
        return new Worker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .config(kinesisClientLibConfiguration)
                .build();
    }

    @Bean
    public IRecordProcessorFactory getRecordProcessorFactory(ConcurrentLinkedQueue<List<String>> processedRecordsList) {
        return new RecordProcessorFactory(processedRecordsList);
    }

    @Bean
    public ApplicationContextAware getAppContext() {
        return new ApplicationContextAware();
    }
}
