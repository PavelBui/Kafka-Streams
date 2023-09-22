package com.epam.learning.messageorientedmiddleware.kafkastreams;

import com.epam.learning.messageorientedmiddleware.kafkastreams.bean.FirstStream;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaStreamsApplicationTests {

	TopologyTestDriver topologyTestDriver = null;
	TestInputTopic<Integer, String> inputTopic = null;
	TestOutputTopic<Integer, String> outputTopic = null;
	StreamsBuilder streamsBuilder;
	FirstStream firstStream = new FirstStream();

	@BeforeEach
	void setUp() {
		streamsBuilder = new StreamsBuilder();
		firstStream.process(streamsBuilder);
		topologyTestDriver = new TopologyTestDriver(streamsBuilder.build());
		inputTopic = topologyTestDriver.createInputTopic("task1-1", Serdes.Integer().serializer(), Serdes.String().serializer());
		outputTopic = topologyTestDriver.createOutputTopic("task1-2", Serdes.Integer().deserializer(), Serdes.String().deserializer());
	}

	@AfterEach
	void tearDown() {
		topologyTestDriver.close();
	}


	@Test
	void testFirstStream() {
		inputTopic.pipeInput(123, "Test");
		Map<Integer, String> keyValueMap = outputTopic.readKeyValuesToMap();
		assertEquals(1, keyValueMap.size());
		assertTrue(keyValueMap.containsKey(123));
		assertTrue(keyValueMap.containsValue("Test"));
	}

}
