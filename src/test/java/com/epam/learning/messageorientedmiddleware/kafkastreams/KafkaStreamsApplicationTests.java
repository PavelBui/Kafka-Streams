package com.epam.learning.messageorientedmiddleware.kafkastreams;

import com.epam.learning.messageorientedmiddleware.kafkastreams.bean.FirstStream;
import com.epam.learning.messageorientedmiddleware.kafkastreams.bean.SecondStream;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaStreamsApplicationTests {

	TopologyTestDriver topologyTestDriver = null;
	TestInputTopic<Integer, String> inputTopic = null;
	TestOutputTopic<Integer, String> outputTopic = null;
	StreamsBuilder streamsBuilder;
	FirstStream firstStream = new FirstStream();
	SecondStream secondStream = new SecondStream();

	@BeforeEach
	void setUp() {
		streamsBuilder = new StreamsBuilder();
	}

	@AfterEach
	void tearDown() {
		topologyTestDriver.close();
	}


	@Test
	void testFirstStream() {
		firstStream.process(streamsBuilder);
		topologyTestDriver = new TopologyTestDriver(streamsBuilder.build());
		inputTopic = topologyTestDriver.createInputTopic("task1-1", Serdes.Integer().serializer(), Serdes.String().serializer());
		outputTopic = topologyTestDriver.createOutputTopic("task1-2", Serdes.Integer().deserializer(), Serdes.String().deserializer());
		inputTopic.pipeInput(123, "Test");
		Map<Integer, String> keyValueMap = outputTopic.readKeyValuesToMap();
		assertEquals(1, keyValueMap.size());
		assertTrue(keyValueMap.containsKey(123));
		assertTrue(keyValueMap.containsValue("Test"));
	}

	@Test
	void testSecondStream() {
		List<String> printMessages = secondStream.process(streamsBuilder);
		topologyTestDriver = new TopologyTestDriver(streamsBuilder.build());
		inputTopic = topologyTestDriver.createInputTopic("task2", Serdes.Integer().serializer(), Serdes.String().serializer());
		inputTopic.pipeInput(123, "Test atesting atester atestereptus tesset");
		printMessages.forEach(System.out::println);
		assertEquals(8, printMessages.size());
		assertEquals(2, printMessages.stream().filter(value -> value.startsWith("short ")).count());
		assertEquals(1, printMessages.stream().filter(value -> value.startsWith("long ")).count());
	}

}
