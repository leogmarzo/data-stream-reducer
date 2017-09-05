/**
 *  Spark Streaming Kafka Log Analyzer.
 */
package com.sparkstreaming.kafka.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import com.databricks.apps.logs.ApacheAccessLog;

public class SparkStreamingKafkaLogAnalyzer {

	private static final Log LOGGER = LogFactory.getLog(SparkStreamingKafkaLogAnalyzer.class);

	// Stats will be computed for the last window length of time.
	private static final Duration WINDOW_LENGTH = new Duration(30 * 1000);

	// Stats will be computed every slide interval time.
	private static final Duration SLIDE_INTERVAL = new Duration(10 * 1000);

	public static void main(String[] args) {
		// Set application name
		String appName = "Spark Streaming Kafka Sample";

		// Create a Spark Context.
		SparkConf conf = new SparkConf()
			.setAppName(appName)
			.setMaster("local[*]")
			.set("spark.executor.memory", "1g");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// This sets the update window to be every 10 seconds.
		JavaStreamingContext jssc = new JavaStreamingContext(sc, SLIDE_INTERVAL); 

		String zkQuorum = "192.168.99.100:2181"; //TODO
		String group = "spark-streaming-sample-groupid";
		String strTopics = "logs";
		int numThreads = 2;

		Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = strTopics.split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> logDataDStream =
                KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        LOGGER.info("Received DStream connecting to zookeeper " + zkQuorum + " group " + group + " topics" +
        		topicMap);
        LOGGER.info("logDataDStream: "+ logDataDStream);

        JavaDStream<ApacheAccessLog> accessLogDStream = logDataDStream.map(
                new Function<Tuple2<String, String>, ApacheAccessLog>() {
                    public ApacheAccessLog call(Tuple2<String, String> message) {
                        String strLogMsg = message._2();
						try {
							return ApacheAccessLog.parseFromLogLine(strLogMsg);
						} catch (IOException e) {
							e.printStackTrace();
						}
						return null;
					}
                }
            );  
        accessLogDStream.print();

        JavaDStream<ApacheAccessLog> windowDStream = accessLogDStream.window(
				WINDOW_LENGTH, SLIDE_INTERVAL);

		System.out.println("PRINT");
		windowDStream.print();
		// Start the streaming server.
		jssc.start(); // Start the computation
		try {
			jssc.awaitTermination(); // Wait for the computation to terminate
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

