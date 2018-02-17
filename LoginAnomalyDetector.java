package auditlog;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka09.ConsumerStrategies;
import org.apache.spark.streaming.kafka09.KafkaUtils;
import org.apache.spark.streaming.kafka09.LocationStrategies;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;
import java.util.Properties;
import java.util.Iterator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import scala.Tuple2;

public class LoginAnomalyDetector {
	// Set the stream and topic to publish to.
	public static String topic;

	// Set the number of messages to send.
	public static int numMessages = 50;

	// Declare a new producer.
	public static KafkaProducer<String, JsonNode> producer;

	public static void main(String[] args)
			throws IOException, ExecutionException, InterruptedException, TimeoutException {
		if (args.length < 5) {
			System.err.println("Usage: LoginAnomalyDetector <broker> <in-topic> <out-topic> <threshold> <interval-in-ms> <window-size-in-ms>");
			System.err.println("eg: LoginAnomalyDetector localhost:9092 auditRecords loginAnomalies 5 5000 30000");
			System.exit(1);
		}

		// set variables from command-line arguments
		final String broker = args[0];
		String inTopic = args[1];
		final String outTopic = args[2];
		final int threshold = Integer.parseInt(args[3]);
		long interval = Long.parseLong(args[4]);
		long window = Long.parseLong(args[5]);

		// hardcode some variables
		String master = "local[*]";
		String consumerGroup = "mycg";

		// define topic to subscribe to
		final Pattern topicPattern = Pattern.compile(inTopic, Pattern.CASE_INSENSITIVE);

		// set Kafka client parameters
		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("bootstrap.servers", broker);
		kafkaParams.put("group.id", consumerGroup);

		// initialize the streaming context
		JavaStreamingContext jssc = new JavaStreamingContext(master, "anomalydetector", new Duration(interval));

		// pull ConsumerRecords out of the stream
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>SubscribePattern(topicPattern, kafkaParams));

		// pull values out of ConsumerRecords
		JavaDStream<String> values = messages.map(new Function<ConsumerRecord<String, String>, String>() {
			private static final long serialVersionUID = 1L;
			public String call(ConsumerRecord<String, String> record) throws Exception {
				return record.value();
			}
		});

		// pull records out of values
		JavaDStream<String> records = values.flatMap(new FlatMapFunction<String, String>() {
			private static final long serialVersionUID = 1L;
			public Iterator<String> call(String x) {
				return Arrays.asList(x.split("\n")).iterator();
			}
		});

		// find failed logins and calculate their number
		JavaPairDStream<String, Integer> failedLogins = records.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;
			public Tuple2<String, Integer> call(String rec) {
				String[] failed_log = rec.split(" ");
				if (failed_log[0].split("=")[1].equals("USER_LOGIN")) {
					if (failed_log[13].split("=")[1].equals("failed'")) {
						return new Tuple2<String, Integer>(failed_log[8].split("=")[1], 1);
					} else
						return new Tuple2<String, Integer>("ignore", 0);
				} else
					return new Tuple2<String, Integer>("ignore", 0);
			}
		}).reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		}, Durations.milliseconds(window), Durations.milliseconds(interval));
		failedLogins.print();

		// send the number of failed logins to the output stream
		failedLogins.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
			private static final long serialVersionUID = 2700738329774962618L;
			public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
					private static final long serialVersionUID = -250139202220821945L;
					public void call(Iterator<Tuple2<String, Integer>> tupleIterator) throws Exception {
						// configure producer properties
						Properties producerProps = new Properties();
						producerProps.put("bootstrap.servers", broker);
						producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
						producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

						// instantiate the producer once per partition
						KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProps);

						// produce key value record
						while (tupleIterator.hasNext()) {
							Tuple2<String, Integer> tuple = tupleIterator.next();
							// consider only failed logins (!=ignore)
							if (!tuple._1.toString().equals("ignore")) {
								// number of failed logins must be greater than the threshold value
								if (tuple._2 >= threshold) {
									ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>
										(outTopic, tuple._1.toString(), "\n User: " + tuple._1.toString() + "\n Number of failed logins: " + tuple._2.toString());
									producer.send(producerRecord);
								}
							}
						}
						// close the producer per partition
						producer.close();
					}
				});
			}
		});

		// start the consumer
		jssc.start();

		// stay in infinite loop until terminated
		try {
			jssc.awaitTermination();
		} catch(InterruptedException e) {
		}
	}

	public static void configureProducer(String brokerPort) {
		Properties props = new Properties();
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("bootstrap.servers", "localhost:" + brokerPort);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, JsonNode>(props);
	}
}