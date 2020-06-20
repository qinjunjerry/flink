package org.apache.flink.streaming.examples.kafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import java.util.Properties;

/**
 * This is a Kafka Test.
 */
public class KafkaExample {

	static final String TOPIC_IN = "input-topic";
	static final String TOPIC_OUT = "output-topic";

	static class SimpleKafkaSerSchema implements KafkaSerializationSchema<String> {

		@Override
		public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
			return new ProducerRecord<>(TOPIC_OUT, element.getBytes());
		}
	}

	public static void main(String[] args) throws Exception {

		// Flink environment setup
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));

		// Flink check/save point setting
		env.enableCheckpointing(60000);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10000);
		env.getCheckpointConfig().setCheckpointTimeout(150000);
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

		env.getCheckpointConfig().enableExternalizedCheckpoints(
				CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
		);

		// Properties for Kafka
		Properties kafkaConsumerProps = new Properties();
		kafkaConsumerProps.setProperty("bootstrap.servers", "localhost:9092");
		kafkaConsumerProps.setProperty("group.id", "flinkconsumer");

		FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<String>(
				TOPIC_IN,
				new SimpleStringSchema(),
				kafkaConsumerProps);

		// Properties for Kafka
		Properties kafkaProducerProps = new Properties();
		kafkaProducerProps.setProperty("bootstrap.servers", "localhost:9092");
		kafkaProducerProps.setProperty("transaction.timeout.ms", "7200000");
		//kafkaProducerProps.setProperty("max.block.ms", "7200000");

		FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
				TOPIC_OUT,
				new SimpleKafkaSerSchema(),
				kafkaProducerProps,
				FlinkKafkaProducer.Semantic.EXACTLY_ONCE
		);

		DataStream<String> stream = env.addSource(consumer);
		stream.addSink(producer);

		env.execute();
	}
}
