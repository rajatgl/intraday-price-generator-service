package org.headstrait.intradaypricegeneratortest.testconsumer.deserializer;

import org.apache.kafka.common.serialization.Deserializer;
import org.headstrait.intradaypricegenerator.model.CsvGeneratorEventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

public class EventDeserializer implements Deserializer<CsvGeneratorEventDTO> {

    private Logger log = LoggerFactory.getLogger(EventDeserializer.class);
    private ObjectMapper objectMapper;

    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        objectMapper = new ObjectMapper();
    }

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic topic associated with the data
     * @param data  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    @Override
    public CsvGeneratorEventDTO deserialize(String topic, byte[] data) {
        try {
            log.info("Deserializing.....");
            return objectMapper.readValue(data, CsvGeneratorEventDTO.class);
        } catch (final IOException e) {
            log.error("Could not deserialize......");
            throw new IllegalArgumentException(e);
        }
    }
}
