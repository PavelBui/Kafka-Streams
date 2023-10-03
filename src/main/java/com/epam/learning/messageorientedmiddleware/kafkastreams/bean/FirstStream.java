package com.epam.learning.messageorientedmiddleware.kafkastreams.bean;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.apache.kafka.streams.kstream.Produced.with;

@Component
public class FirstStream {

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
        streamsBuilder.stream("task1-1", Consumed.with(Serdes.Integer(), Serdes.String()))
//                .mapValues(value -> value = value + "123")
                .to("task1-2", with(Serdes.Integer(), Serdes.String()));
    }
}
