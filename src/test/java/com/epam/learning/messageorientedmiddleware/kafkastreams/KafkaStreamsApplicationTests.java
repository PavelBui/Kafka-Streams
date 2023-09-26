package com.epam.learning.messageorientedmiddleware.kafkastreams;

import com.epam.learning.messageorientedmiddleware.kafkastreams.bean.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaStreamsApplicationTests {

	TopologyTestDriver topologyTestDriver = null;
	TestInputTopic<Integer, String> inputTopic = null;
	TestInputTopic<Integer, String> inputSecondTopic = null;
	TestInputTopic<Integer, Person> inputPersonTopic = null;
	TestOutputTopic<Integer, String> outputTopic = null;
	StreamsBuilder streamsBuilder;
	FirstStream firstStream = new FirstStream();
	SecondStream secondStream = new SecondStream();
	ThirdStream thirdStream = new ThirdStream();

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

	@Test
	void testThirdStream() throws InterruptedException {
		List<String> printMessages = thirdStream.process(streamsBuilder);
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
		topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), props);
		inputTopic = topologyTestDriver.createInputTopic("task3-1", Serdes.Integer().serializer(), Serdes.String().serializer());
		inputSecondTopic = topologyTestDriver.createInputTopic("task3-2", Serdes.Integer().serializer(), Serdes.String().serializer());
		inputTopic.pipeInput(12345, "1:Test 1 left");
		inputSecondTopic.pipeInput(123, "1:Test 1 right");
		inputTopic.pipeInput(345, "2:Test 2 left");
		inputTopic.pipeInput(3456, "3:Test 3 left");
		inputTopic.pipeInput(3457, "4:Test 4 left");
		Thread.sleep(30000);
		inputSecondTopic.pipeInput(1235, "2:Test 2 right");
		Thread.sleep(20000);
		inputSecondTopic.pipeInput(1236, "3:Test 3 right");
		Thread.sleep(10000);
		inputSecondTopic.pipeInput(1237, "4:Test 4 right");
		printMessages.forEach(System.out::println);
		assertEquals(11, printMessages.size());
		assertEquals(3, printMessages.stream().filter(value -> value.contains("#")).count());
	}

	@Test
	void testFourthStream() {
		List<String> printMessages = thirdStream.process(streamsBuilder);
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.serdeFrom(new CustomSerializer(), new CustomDeserializer()));
		topologyTestDriver = new TopologyTestDriver(streamsBuilder.build(), props);
		inputPersonTopic = topologyTestDriver.createInputTopic("task4", Serdes.Integer().serializer(), new CustomSerializer());
		inputPersonTopic.pipeInput(12345, new Person("Pavel", "EPAM", "Developer", 2));
		printMessages.forEach(System.out::println);
//		assertEquals(8, printMessages.size());
//		assertEquals(2, printMessages.stream().filter(value -> value.startsWith("short ")).count());
//		assertEquals(1, printMessages.stream().filter(value -> value.startsWith("long ")).count());
	}
}
