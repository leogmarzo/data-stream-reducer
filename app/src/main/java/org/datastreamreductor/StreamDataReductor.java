/**
 *  Spark Streaming Kafka Log Analyzer.
 */
package org.datastreamreductor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class StreamDataReductor implements Serializable{

	private static final Log LOGGER = LogFactory.getLog(StreamDataReductor.class);

	// Stats will be computed for the last window length of time.
	private static final Duration WINDOW_LENGTH = new Duration(30 * 1000);

	// Stats will be computed every slide interval time.
	private static final Duration SLIDE_INTERVAL = new Duration(10 * 1000);

	public static void main(String[] args) {
		new StreamDataReductor().run();
	}

		public void run() {

		// Set application name
		String appName = "Stream Data Reductor";

		// Create a Spark Context.
		SparkConf conf = new SparkConf()
				.setAppName(appName)
				.setMaster("spark://master:7077")
				.set("spark.executor.memory", "2g");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// This sets the update window to be every 10 seconds.
		JavaStreamingContext jssc = new JavaStreamingContext(sc, SLIDE_INTERVAL);

		String zkQuorum = "192.168.99.100:2181"; //TODO
		String group = "spark-streaming-sample-groupid";
		String strTopics = "logs";
		int numThreads = 2;

		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		String[] topics = strTopics.split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}

		JavaPairReceiverInputDStream<String, String> logDataDStream =
				KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

		LOGGER.info("Received DStream connecting to zookeeper " + zkQuorum + " group " + group + " topics" +
				topicMap);

		logDataDStream.print();

		JavaDStream<LogEntry> logDStream = logDataDStream.map(
				new Function<Tuple2<String, String>, LogEntry>() {
					public LogEntry call(Tuple2<String, String> message) {
						String strLogMsg = message._2();
						String[] eventParts = strLogMsg.split("]", 2);
						//String[] eventParts = new String[]{"aaa", "bbb"};
						String date = eventParts[0];
						String stackTrace = eventParts[1];
						int hash = stackTrace.hashCode();
						return new LogEntry(hash, date, stackTrace);
					}
				}
		);

		//store in redis
		logDStream.foreachRDD(
				new VoidFunction2<JavaRDD<LogEntry>, Time>() {
					@Override
					public void call(JavaRDD<LogEntry> logEntryJavaRDD, Time time) throws Exception {
						logEntryJavaRDD.foreach(
								new VoidFunction<LogEntry>() {
									@Override
									public void call(LogEntry logEntry) throws Exception {
										pushToRedis(logEntry);
									}
								}
						);
					}
				}
		);

		jssc.start(); // Start the computation

		try {
			jssc.awaitTermination(); // Wait for the computation to terminate
		} catch (InterruptedException e) {
			LOGGER.error("error on termination", e);
		}
	}

	private static void pushToRedis(LogEntry logEntry) {
		JedisPool jedisPool = new JedisPool("192.168.99.100", 6379);
		Jedis jedis = jedisPool.getResource();
		LOGGER.info("Guardando log " + logEntry.getStackTrace());
		jedis.set("log:" + logEntry.getDate(), logEntry.getHash()+"");
		jedis.set("hashes:" + logEntry.getHash(), logEntry.getStackTrace());

		jedis.close();
		jedisPool.close();
	}
}
