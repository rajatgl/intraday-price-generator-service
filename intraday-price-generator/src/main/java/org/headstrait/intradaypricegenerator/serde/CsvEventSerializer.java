package org.headstrait.intradaypricegenerator.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.headstrait.intradaypricegenerator.model.CsvGeneratorEventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvEventSerializer implements Serializer<CsvGeneratorEventDTO> {

    private final Logger log = LoggerFactory.getLogger(CsvEventSerializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with data
     * @param data  typed data
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(String topic, CsvGeneratorEventDTO data) {
        try{
            if(data == null){
                log.warn("Null received at serializer");
                return new byte[0];
            }
            log.info("Serializing csv generator request....");
            return objectMapper.writeValueAsBytes(data);
        }catch (Exception e){
            e.printStackTrace();
            throw new SerializationException("Error when serializing CSV generator request Event to bytes.");
        }
    }
}
