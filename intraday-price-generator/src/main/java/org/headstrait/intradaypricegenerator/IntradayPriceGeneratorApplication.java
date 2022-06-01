package org.headstrait.intradaypricegenerator;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.headstrait.intradaypricegenerator.model.CsvGeneratorEventDTO;
import org.headstrait.intradaypricegenerator.model.S3UploaderRequestEventDTO;
import org.headstrait.intradaypricegenerator.serde.CsvEventDeserializer;
import org.headstrait.intradaypricegenerator.serde.CsvEventSerializer;
import org.headstrait.intradaypricegenerator.serde.S3UploaderRequestEventSerializer;
import org.headstrait.intradaypricegenerator.service.*;
import org.headstrait.intradaypricegenerator.service.interfaces.IOhlcReader;
import org.headstrait.intradaypricegenerator.service.interfaces.IProducer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.headstrait.intradaypricegenerator.constants.Constants.*;
import static org.headstrait.intradaypricegenerator.utils.Utilities.await;


public class IntradayPriceGeneratorApplication {



    //set csv event producer properties
    private static Properties csvRequestProducerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                STOCK_SIMULATOR_BOOTSTRAP_BROKER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                CsvEventSerializer.class.getName());
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, REQUEST_SIZE);
        return properties;
    }

    private static Properties s3RequestProducerProperties(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                STOCK_SIMULATOR_BOOTSTRAP_BROKER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                S3UploaderRequestEventSerializer.class.getName());
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, REQUEST_SIZE);
        return properties;
    }

    //stream consumer properties
    private static Properties csvResponseConsumerProperties(){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                STOCK_SIMULATOR_BOOTSTRAP_BROKER);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                System.getenv("STOCK_SIMULATOR_CONSUMER_GROUP_ID"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                CsvEventDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,
                REQUEST_SIZE);
        properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,
                REQUEST_SIZE);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "latest");
        return properties;
    }



    private static final String CSV_GENERATOR_REQUESTS_TOPIC = System.getenv("CSV_GENERATOR_REQUESTS_TOPIC");
    private static final String STREAM_OUTPUT_TOPIC=System.getenv("STREAM_OUTPUT_TOPIC");
    private static final String STREAM_INPUT_TOPIC=System.getenv("STREAM_INPUT_TOPIC");


    //Thread pool of 2
    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(2);

    private static Future<?> initiateFileContentStream(IntradayPricesFileContentStreamer contentStreamer,
                                                    String inputTopic,
                                                    String outputTopic){
        return EXECUTOR.submit(() -> contentStreamer.streamPoll(inputTopic,outputTopic));
    }

    private static Future<?> initiateIntradayPriceProduction(IntradayPricesEventProducer
                                                                  intradayPricesEventProducer,
                                                          String stock){
            return EXECUTOR.submit(() -> {
                try {
                    intradayPricesEventProducer.produce(stock);
                } catch (Exception exception) {
                    exception.printStackTrace();
                }
            });
    }


    public static void main(String[] args) {

        //create kafka producer using the loaded properties
        final Producer<String, CsvGeneratorEventDTO> csvEventProducer = new KafkaProducer<>(csvRequestProducerProperties());
        final Consumer<String, CsvGeneratorEventDTO> csvEventConsumer = new KafkaConsumer<>(csvResponseConsumerProperties());


        final Producer<String, S3UploaderRequestEventDTO> s3EventProducer = new KafkaProducer<>(s3RequestProducerProperties());

        final String ohlcFolderPath = getOhlcFolderPathString();



        //initialize ohlc reader
        IOhlcReader ohlcReader = new OhlcReader(ohlcFolderPath);

        //initialize producer service
        IProducer<String, CsvGeneratorEventDTO> producerService = new ProducerService(csvEventProducer, CSV_GENERATOR_REQUESTS_TOPIC);

        //initialize price generator
        IntradayPriceGenerator intradayPriceGenerator = new IntradayPriceGenerator();

        //subscribe to stream input topic
        csvEventConsumer.subscribe(Collections.singletonList(STREAM_INPUT_TOPIC));

        //initialize content streamer object
        IntradayPricesFileContentStreamer contentStreamer =
                new IntradayPricesFileContentStreamer(s3EventProducer,csvEventConsumer);



        //initialize intraday prices producer
        IntradayPricesEventProducer intradayPricesEventProducer =
                new IntradayPricesEventProducer(ohlcReader, producerService, intradayPriceGenerator);

        Future<?> streamTask = initiateFileContentStream(contentStreamer,STREAM_INPUT_TOPIC,STREAM_OUTPUT_TOPIC);

        List<Future<?>> producerTasks = new ArrayList<>();
        for (String stock:
             getStocks()) {
            //invoke produce method of prices event producer
            Future<?> producerTask = initiateIntradayPriceProduction(intradayPricesEventProducer,stock);
            producerTasks.add(producerTask);
        }

        //await for the tasks to return.
        await(producerTasks);
        await(streamTask);
    }

}