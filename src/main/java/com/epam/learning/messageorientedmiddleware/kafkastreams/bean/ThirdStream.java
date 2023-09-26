package com.epam.learning.messageorientedmiddleware.kafkastreams.bean;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Component
public class ThirdStream {

    @Autowired
    public List<String> process(StreamsBuilder streamsBuilder) {
        List<String> printMessages = new ArrayList<>();
        KStream<Integer, String> inputFirstMessage = streamsBuilder.stream("task3-1", Consumed.with(Serdes.Integer(), Serdes.String()))
                .filter((key,value) -> value != null && value.contains(":"))
                .flatMap((key,value) -> {
                    List<KeyValue<Integer, String>> result = new ArrayList<>();
                    String[] messageParts = value.split(":");
                    int newKey = Integer.parseInt(messageParts[0]);
                    printMessages.add("{" + newKey + " - " + messageParts[1] + "}");
                    result.add(KeyValue.pair(newKey, messageParts[1]));
                    return result;
                });
        KStream<Integer, String> inputSecondMessage = streamsBuilder.stream("task3-2", Consumed.with(Serdes.Integer(), Serdes.String()))
                .filter((key,value) -> value != null && value.contains(":"))
                .flatMap((key,value) -> {
                    List<KeyValue<Integer, String>> result = new ArrayList<>();
                    String[] messageParts = value.split(":");
                    int newKey = Integer.parseInt(messageParts[0]);
                    printMessages.add("{" + newKey + " - " + messageParts[1] + "}");
                    result.add(KeyValue.pair(newKey, messageParts[1]));
                    return result;
                });
        inputFirstMessage.join(inputSecondMessage,
                (leftValue, rightValue) -> leftValue + "#" + rightValue,
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(30)))
                .foreach(((key, value) -> printMessages.add("{" + key + " - " + value + "}")));

        return printMessages;
    }
}
