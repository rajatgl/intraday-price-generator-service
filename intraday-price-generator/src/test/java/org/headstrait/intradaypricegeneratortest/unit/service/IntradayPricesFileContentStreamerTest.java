package org.headstrait.intradaypricegeneratortest.unit.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.headstrait.intradaypricegenerator.model.CsvGeneratorEventDTO;
import org.headstrait.intradaypricegenerator.model.S3UploaderRequestEventDTO;
import org.headstrait.intradaypricegenerator.serde.S3UploaderRequestEventSerializer;
import org.headstrait.intradaypricegenerator.service.IntradayPricesFileContentStreamer;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.stream.Collectors;

import static org.headstrait.intradaypricegeneratortest.constants.TestObjects.CSV_GENERATOR_EVENT_DTO;
import static org.headstrait.intradaypricegeneratortest.constants.TestObjects.S3_UPLOADER_REQUEST_DTO;
import static org.junit.Assert.assertEquals;

public class IntradayPricesFileContentStreamerTest {

    private String inputTopic;
    private String outputTopic;
    private MockConsumer<String, CsvGeneratorEventDTO> consumer;
    private MockProducer<String, S3UploaderRequestEventDTO> producer;
    private IntradayPricesFileContentStreamer intradayPricesFileContentStreamer;

    @Before
    public void setUp() {

        inputTopic = "CSV_GENERATOR_RESPONSES";
        outputTopic = "S3_UPLOADER_REQUESTS";

        //mock consumer
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

        //mock producer
        producer = new MockProducer<>(true, new StringSerializer(), new S3UploaderRequestEventSerializer());

        consumer.subscribe(Collections.singleton(inputTopic));

        //manual invoking required for mock consumer to rebalance partitions
        consumer.rebalance(Arrays.asList(new TopicPartition(inputTopic, 0),
                new TopicPartition(inputTopic, 1)));


        //mock consumers need to seek manually since they cannot automatically reset offsets
        HashMap<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(new TopicPartition(inputTopic, 0), 0L);
        endOffsets.put(new TopicPartition(inputTopic, 1), 0L);

        consumer.updateEndOffsets(endOffsets);
        consumer.seek(new TopicPartition(inputTopic, 0), 0);

        ConsumerRecord<String, CsvGeneratorEventDTO> rec1 = new ConsumerRecord<>(inputTopic, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, "key1", CSV_GENERATOR_EVENT_DTO);

        consumer.addRecord(rec1);


        //inject the mocked producer and consumer in place of actual into file streamer for test purpose
        intradayPricesFileContentStreamer = new IntradayPricesFileContentStreamer(producer, consumer);
    }

    @Test
    public void streamMethod_ShouldStreamTheConsumedEventFromInputTopicOntoOutputTopic_WhenInvoked() {
        //invoke the method
        intradayPricesFileContentStreamer.stream(outputTopic);
        S3UploaderRequestEventDTO actualEventsSent =
                producer.history().stream().map(this::toValue)
                        .collect(Collectors.toList()).get(0);
        S3UploaderRequestEventDTO expectedEvents = S3_UPLOADER_REQUEST_DTO;

        //check if the files added as consumer records match the one recorded in producer's record history
        assertEquals(expectedEvents,actualEventsSent);
    }

    @Test
    public void pollStreamMethod_ShouldContinueToPollTillHeartbeatValueIsNotZeroAndStreamTheConsumedEventFromInputTopicOntoOutputTopic_WhenInvoked() {
        //invoke the method
        intradayPricesFileContentStreamer.streamPoll(inputTopic,outputTopic);
        S3UploaderRequestEventDTO actualEventsSent =
                producer.history().stream().map(this::toValue)
                        .collect(Collectors.toList()).get(0);
        S3UploaderRequestEventDTO expectedEvents = S3_UPLOADER_REQUEST_DTO;

        //check if the files added as consumer records match the one recorded in producer's record history
        assertEquals(expectedEvents,actualEventsSent);
    }


    //get value form producer record
    private S3UploaderRequestEventDTO toValue(final ProducerRecord<String, S3UploaderRequestEventDTO> producerRecord) {
        return producerRecord.value();
    }

}
