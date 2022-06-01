package org.headstrait.intradaypricegenerator.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.headstrait.intradaypricegenerator.model.CsvGeneratorEventDTO;
import org.headstrait.intradaypricegenerator.model.S3UploaderRequestEventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Future;

public class IntradayPricesFileContentStreamer {

    private final Logger logger = LoggerFactory.getLogger(IntradayPricesFileContentStreamer.class);

    private final Producer<String, S3UploaderRequestEventDTO> producer;
    private final Consumer<String, CsvGeneratorEventDTO> consumer;

    private static final int HEARTBEATS = 1000;

    public IntradayPricesFileContentStreamer(Producer<String, S3UploaderRequestEventDTO> producer, Consumer<String, CsvGeneratorEventDTO> consumer) {
        this.producer = producer;
        this.consumer = consumer;

    }

    //poll once
    /**
     * stream the consumed content onto the output topic
     * @param outputTopic on which the processed conntent is to be produced.
     * @return true if successful else false
     */
    public boolean stream(String outputTopic){
        ConsumerRecords<String, CsvGeneratorEventDTO> consumerRecords = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, CsvGeneratorEventDTO> consumerRecord : consumerRecords) {
            String key = consumerRecord.key();
            logger.info("CONSUMED Key: {}", key);
            String[] splitFileName = consumerRecord.value().getFileName()
                    .split("-");
            String filePath = splitFileName[3]+"-"+splitFileName[4]+"-"+splitFileName[5].split("\\.")[0]+
                    "/"+splitFileName[0].toUpperCase();
            String bucketName = "s3-uploader-bucket-test-1";
            logger.info("S3 FILE PATH: {}",filePath);
            S3UploaderRequestEventDTO s3UploaderRequestEvent = S3UploaderRequestEventDTO.builder()
                    .fileName(consumerRecord.value().getFileName())
                    .requestId(consumerRecord.value().getRequestId())
                    .headers(consumerRecord.value().getHeaders())
                    .data(consumerRecord.value().getData())
                    .numberOfRows(consumerRecord.value().getNumberOfRows())
                    .s3FilePath(filePath)
                    .bucketName(bucketName)
                    .build();
            ProducerRecord<String, S3UploaderRequestEventDTO> producerRecord =
                    new ProducerRecord<>(outputTopic, key, s3UploaderRequestEvent);
            Future<RecordMetadata> metadataFuture = producer.send(producerRecord);
            try {
                RecordMetadata metadata = metadataFuture.get();
                logger.info("\nSENT FILE WITH KEY: {} OTO TOPIC: {} at OFFSET: {} and TIMESTAMP: {}\n",
                        key,
                        outputTopic,
                        metadata.offset(),
                        metadata.timestamp());
            } catch (Exception exception) {
                logger.info("KAFKA ERROR: {}", exception.getMessage());
                exception.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
        return !consumerRecords.isEmpty();
    }

    //poll for incoming consumer events continuously and stream it onto another topic
    /**
     *
     * @param inputTopic form which records are consumed.
     * @param outputTopic to which processed records are produced.
     */
    public void streamPoll(final String inputTopic, final String outputTopic) {
        logger.info("LISTENING TO TOPIC: {} ......", inputTopic);
        int heartBeats = HEARTBEATS;
        //polling
        while (heartBeats >= 0) {
           boolean status = stream(outputTopic);
           if(!status) {
               heartBeats--;
           }
           else {
               heartBeats = HEARTBEATS;
           }
        }
    }
}



