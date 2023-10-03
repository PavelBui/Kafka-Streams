package com.epam.learning.messageorientedmiddleware.kafkastreams.serde;

import com.epam.learning.messageorientedmiddleware.kafkastreams.bean.Person;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class MySerializer implements Serializer<Person> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Person person) {
        try {
            if (person == null){
                System.out.println("Null received at serializing");
                return null;
            }
            return objectMapper.writeValueAsBytes(person);
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Person to byte[]");
        }
    }

    @Override
    public void close() {
    }
}