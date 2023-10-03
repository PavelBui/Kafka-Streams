package com.epam.learning.messageorientedmiddleware.kafkastreams.bean;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class SecondStream {

    @Autowired
    public List<String> process(StreamsBuilder streamsBuilder) {
        List<String> printMessages = new ArrayList<>();
        KStream<Integer, String> inputMessage = streamsBuilder.stream("task2", Consumed.with(Serdes.Integer(), Serdes.String()))
                .filter((key,value) -> value != null)
                .flatMap((key, value) -> {
                    List<KeyValue<Integer, String>> result = new ArrayList<>();
                    for (String word : value.split(" ")) {
                        printMessages.add("{" + word.length() + " - " + word + "}");
                        result.add(KeyValue.pair(word.length(), word));
                    }
                    return result;
                });

        KStream<Integer, String> wordsShort = inputMessage
                .filter((key, value) -> key < 10);
        KStream<Integer, String> wordsLong = inputMessage
                .filter((key, value) -> key >= 10);

        Map<String, KStream<Integer, String>> messages = new HashMap<>();
        messages.put("short", wordsShort.filter((key, value) -> value.contains("a")));
        messages.put("long", wordsLong.filter((key, value) -> value.contains("a")));
        messages.forEach((mkey, mvalue) -> mvalue.foreach((key, value) -> printMessages.add(mkey + " {" + key + " : " + value + "}")));

        return printMessages;
    }
}
