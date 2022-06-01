package org.headstrait.intradaypricegeneratortest.unit.service;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.headstrait.intradaypricegenerator.model.CsvGeneratorEventDTO;
import org.headstrait.intradaypricegenerator.serde.CsvEventSerializer;
import org.headstrait.intradaypricegenerator.service.*;
import org.headstrait.intradaypricegenerator.service.interfaces.IOhlcReader;
import org.headstrait.intradaypricegenerator.service.interfaces.IProducer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.stream.Collectors;

import static org.headstrait.intradaypricegeneratortest.constants.TestObjects.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class IntradayPricesEventProducerTest {

    private IntradayPricesEventProducer intradayPricesEventProducer;

    private IOhlcReader ohlcReader;

    private IProducer<String, CsvGeneratorEventDTO> producer;

    private IntradayPriceGenerator intradayPriceGenerator;

    //random test stock
    private final String stock = "STOCK.NS";

    //random test topic
    private final String topic = "CSV_GENERATOR_REQUESTS";

    //mock producer
    private final MockProducer<String, CsvGeneratorEventDTO> mockProducer = new MockProducer<>(
            true,
            new StringSerializer(),
            new CsvEventSerializer());

    @Before
    public void setUp(){
        ohlcReader = mock(OhlcReader.class);
        producer = new ProducerService(mockProducer,topic);

        //utilize a mock producer to construct producerService
        //and check whether producer record contains the event being sent
        intradayPriceGenerator = mock(IntradayPriceGenerator.class);
        intradayPricesEventProducer = new IntradayPricesEventProducer(ohlcReader,producer, intradayPriceGenerator);
    }

    @Test
    public void produce_ShouldSendTheGeneratedPriceListEventForEachDay_WhenInvoked() throws ParseException, IOException {
        //mock the ohlc reader to produce list of our choice for unit test purpose
        when(ohlcReader.getOhlcDataForAStock(stock)).thenReturn(OHLC_LIST);
        //mock the price generator to produce prices of our choice for unit test purpose
        when(intradayPriceGenerator.getRandomPrices(stock,OHLC,true)).thenReturn(INTRADAY_PRICES);

        intradayPricesEventProducer.produce(stock);
        CsvGeneratorEventDTO actualEventSent =
                mockProducer.history().stream().map(this::toValue).collect(Collectors.toList()).get(0);
        CsvGeneratorEventDTO expectedEvent = CSV_GENERATOR_EVENT_DTO;
        //check if the headers match
        assertEquals(expectedEvent.getHeaders(),actualEventSent.getHeaders());
        //check if the data match
        assertEquals(expectedEvent.getData(),actualEventSent.getData());
    }

    private CsvGeneratorEventDTO toValue(final ProducerRecord<String, CsvGeneratorEventDTO> producerRecord) {
        return producerRecord.value();
    }
}
