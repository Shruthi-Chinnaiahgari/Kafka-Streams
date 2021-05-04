package com.demo.kafka;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;

public class StreamsFilterTweets {

	public static void main(String[] args) {

		//create properties
		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		//Like adding group name for consumer
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "twitter-filter-tweets-stream");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		
		//create topology
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		
		//create input topic
		KStream<String, String> inputStream = streamsBuilder.stream("TwitterTweets");
		KStream<String, String> filteredStream = inputStream.filter((k, tweetJson) -> 
			filterMostFollowedTweet(tweetJson) > 10000);
		
		filteredStream.to("ImportantTweets");
		
		//build the topology
		KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
	
		//start the stream
		kafkaStreams.start();

	}
	
	private static JsonParser parser = new JsonParser();
	private static int filterMostFollowedTweet(String json) {
		try {
			return parser.parse(json)
					.getAsJsonObject()
					.get("user")
					.getAsJsonObject()
					.get("followers_count")
					.getAsInt();
		}catch(Exception e){
			return 0;
		}
	}

}
