package org.headstrait.intradaypricegeneratortest;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.headstrait.intradaypricegenerator.model.CsvGeneratorEventDTO;
import org.headstrait.intradaypricegenerator.serde.CsvEventDeserializer;
import org.headstrait.intradaypricegenerator.serde.CsvEventSerializer;
import org.headstrait.intradaypricegenerator.service.IntradayPriceGenerator;
import org.headstrait.intradaypricegenerator.service.IntradayPricesEventProducer;
import org.headstrait.intradaypricegenerator.service.OhlcReader;
import org.headstrait.intradaypricegenerator.service.ProducerService;
import org.headstrait.intradaypricegenerator.service.interfaces.IOhlcReader;
import org.headstrait.intradaypricegenerator.service.interfaces.IProducer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.headstrait.intradaypricegeneratortest.constants.TestObjects.HEADERS;
import static org.headstrait.intradaypricegeneratortest.constants.TestObjects.OHLC_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IntradayPriceGeneratorApplicationIT {


    //logger
    Logger log = LoggerFactory.getLogger(IntradayPriceGeneratorApplicationIT.class);

    private static final String TOPIC = "CSV_GENERATOR_REQUESTS";

    //test container for IT
    KafkaContainer kafkaContainer;

    IOhlcReader ohlcReader;
    IntradayPricesEventProducer intradayPricesEventProducer;
    Consumer<String, CsvGeneratorEventDTO> consumer;

    //formats
    private final SimpleDateFormat destinationDateFormat = new SimpleDateFormat("dd/MM/yyyy");
    private final SimpleDateFormat sourceDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private final SimpleDateFormat destinationTimeFormat = new SimpleDateFormat("HH:mm:ss.SSSSSSSSS");

    //method to create specified topic in the test container
    void createTopics(String topic) {
        List<NewTopic> newTopics = List.of(new NewTopic(topic,
                1,
                (short) 1));
        try (var admin = AdminClient.create(Map.of(BOOTSTRAP_SERVERS_CONFIG,
                kafkaContainer.getBootstrapServers()))) {
            admin.createTopics(newTopics);
        }
    }

    //set producer properties
    Properties producerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaContainer.getBootstrapServers());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                CsvEventSerializer.class.getName());
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "10000000");
        return properties;
    }

    //set consumer properties
    Properties consumerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaContainer.getBootstrapServers());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "prices-consumer-group");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                CsvEventDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "10000000");
        properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "10000000");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }


    @Before
    public void setUp() {

        //create kafka test container
        kafkaContainer = new KafkaContainer(
                DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                .withEnv("KAFKA_MESSAGE_MAX_BYTES", "10000000");

        //start test container
        kafkaContainer.start();
        ohlcReader = mock(OhlcReader.class);

        createTopics(TOPIC);
    }

    @After
    public void teardown() {
        //close the test container
        try {
            kafkaContainer.close();
        } catch (Exception ignored) {
        }
    }


    @Test
    public void produceMethod_ShouldSendThePricesEventOnTheKafkaBroker_WhenInvoked() throws IOException, ParseException {

        //kafka consumer for test
        consumer = new KafkaConsumer<>(consumerProperties());

        //subscribe
        consumer.subscribe(Collections.singletonList(TOPIC));


        //create kafka producer using the loaded properties
        final Producer<String, CsvGeneratorEventDTO> PRODUCER =
                new KafkaProducer<>(producerProperties());

        //initialize the dependency classes for event producer service
        IProducer<String, CsvGeneratorEventDTO> producerService = new ProducerService(PRODUCER, TOPIC);
        IntradayPriceGenerator intradayPriceGenerator = new IntradayPriceGenerator();

        //mock ohlc reader to not read actual folder and return list of choice
        when(ohlcReader.getOhlcDataForAStock("STOCK.NS"))
                .thenReturn(OHLC_LIST);

        //initiailize the event producer
        intradayPricesEventProducer =
                new IntradayPricesEventProducer(ohlcReader, producerService, intradayPriceGenerator);


        //invoke produce method of event producer service for IT
        intradayPricesEventProducer.produce("STOCK.NS");


        //poll consumer for events
        ConsumerRecords<String, CsvGeneratorEventDTO> records = consumer.poll(Duration.ofMillis(5000));

        //check the consumer record for expected outcome
        log.info("checking the consumer record for expected outcome.......");
        records.forEach(record -> {

            int sumOfSizes = 0;
            CsvGeneratorEventDTO csvGeneratorEventDTO = record.value();


            //check if consumer record is null
            log.info("checking if the record received is null...");
            assertNotNull(record);

            //check the consumer record key
            log.info("checking the record key...");
            assertEquals("2023-06-05", record.key());
            //check if the value is null
            log.info("checking if the requestId is null...");
            assertNotNull(record.value().getRequestId());
            //check the header field of eventDTO
            log.info("checking the record headers....");
            assertEquals(HEADERS, record.value().getHeaders());
        });
    }
}