package org.headstrait.intradaypricegeneratortest.unit.service;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.headstrait.intradaypricegenerator.model.CsvGeneratorEventDTO;
import org.headstrait.intradaypricegenerator.serde.CsvEventSerializer;
import org.headstrait.intradaypricegenerator.service.ProducerService;
import org.junit.Before;
import org.junit.Test;

import java.util.stream.Collectors;

import static org.headstrait.intradaypricegeneratortest.constants.TestObjects.*;
import static org.junit.Assert.assertEquals;

public class ProducerServiceTest {

    private ProducerService producerService;

    //mock producer
    final MockProducer<String, CsvGeneratorEventDTO> mockProducer = new MockProducer<>(
            true,
            new StringSerializer(),
            new CsvEventSerializer());

    //random topic for ut
    final String topic = "CSV_GENERATOR_REQUESTS";

    @Before
    public void setUp(){
        //initialize producer service
        producerService = new ProducerService(mockProducer,topic);
    }


    //check whether the events sent onto the kafka topic is same as the one sent to producer service
    @Test
    public void sendMethod_ShouldSendIntradayPricesEventDTO(){
        producerService.send(OHLC.getDate(), CSV_GENERATOR_EVENT_DTO);
        CsvGeneratorEventDTO actualEventSentByTheProducerService =
                mockProducer.history().stream().map(this::toValue).collect(Collectors.toList()).get(0);
        CsvGeneratorEventDTO expectedEvent = CSV_GENERATOR_EVENT_DTO;
        assertEquals(expectedEvent,actualEventSentByTheProducerService);
    }



    //get value form producer record
    private CsvGeneratorEventDTO toValue(final ProducerRecord<String, CsvGeneratorEventDTO> producerRecord) {
        return producerRecord.value();
    }
}
