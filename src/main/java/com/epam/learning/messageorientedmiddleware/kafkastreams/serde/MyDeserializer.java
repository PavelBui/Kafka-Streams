package com.epam.learning.messageorientedmiddleware.kafkastreams.serde;

import com.epam.learning.messageorientedmiddleware.kafkastreams.bean.Person;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class MyDeserializer implements Deserializer<Person> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Person deserialize(String topic, byte[] data) {
        try {
            if (data == null){
                System.out.println("Null received at deserializing");
                return null;
            }
            return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), Person.class);
        } catch (Exception e) {
            throw new SerializationException("Error when deserializing byte[] to Person");
        }
    }

    @Override
    public void close() {
    }
}
