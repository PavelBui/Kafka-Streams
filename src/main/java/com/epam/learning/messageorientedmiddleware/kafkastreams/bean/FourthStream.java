package com.epam.learning.messageorientedmiddleware.kafkastreams.bean;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class FourthStream {

    @Autowired
    public List<String> process(StreamsBuilder streamsBuilder) {
        List<String> printMessages = new ArrayList<>();
        streamsBuilder.stream("task4", Consumed.with(Serdes.Integer(), Serdes.serdeFrom(new CustomSerializer(), new CustomDeserializer())))
                .peek((key,value) -> printMessages.add("{" + key + " - " + value + "}"));

        return printMessages;
    }
}
