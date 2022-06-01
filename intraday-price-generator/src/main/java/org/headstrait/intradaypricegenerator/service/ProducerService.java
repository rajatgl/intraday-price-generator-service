package org.headstrait.intradaypricegenerator.service;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.headstrait.intradaypricegenerator.model.CsvGeneratorEventDTO;
import org.headstrait.intradaypricegenerator.service.interfaces.IProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;

public class ProducerService implements IProducer<String, CsvGeneratorEventDTO> {


    private final Producer<String, CsvGeneratorEventDTO> producer;
    private final String topic;
    private final Logger logger = LoggerFactory.getLogger(ProducerService.class);

    public ProducerService(final Producer<String, CsvGeneratorEventDTO> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    /**
     *
     * @param key of the event to be streamed
     * @param value of the event to be streamed
     */
    @Override
    public void send(String key, CsvGeneratorEventDTO value){
        ProducerRecord<String, CsvGeneratorEventDTO> producerRecord =
                new ProducerRecord<>(topic,key,value);
        Future<RecordMetadata> metadataFuture =  producer.send(producerRecord);
        try {
            RecordMetadata metadata = metadataFuture.get();
            logger.info("\nSENT INTRADAY PRICES FOR DAY: {} TO TOPIC: {}, with REQUEST_ID: {} with OFFSET: {} at: {}\n",
                    key,
                    topic,
                    value.getRequestId(),
                    metadata.offset(),
                    metadata.timestamp());
        }catch (Exception exception){
            logger.info("KAFKA ERROR: {}",exception.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    /**
     * shuts down the kafka producer after use
     */
    @Override
    public void shutdown() {
        producer.close();
    }
}
