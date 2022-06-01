package org.headstrait.intradaypricegenerator.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.headstrait.intradaypricegenerator.model.S3UploaderRequestEventDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3UploaderRequestEventSerializer implements Serializer<S3UploaderRequestEventDTO> {

    private final Logger log = LoggerFactory.getLogger(S3UploaderRequestEventSerializer.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with data
     * @param data  typed data
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(String topic, S3UploaderRequestEventDTO data) {
        try{
            if(data == null){
                log.warn("Null received at serializer");
                return new byte[0];
            }
            log.info("Serializing s3 uploader request....");
            return objectMapper.writeValueAsBytes(data);
        }catch (Exception e){
            e.printStackTrace();
            throw new SerializationException("Error when serializing S3 Uploader Request Event to bytes.");

        }
    }
}
