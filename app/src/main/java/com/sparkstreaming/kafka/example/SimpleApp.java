package com.sparkstreaming.kafka.example;

/**
 * Created by ext_lmarzo on 9/4/17.
 */
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

import java.util.logging.Logger;

public class SimpleApp {

    private static final Logger LOGGER = Logger.getLogger(SimpleApp.class.getName());

    public static void main(String[] args) {
        String logFile = "README.md"; // Should be some file on your system
        System.out.println();
        LOGGER.info("running");
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("a"); }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("b"); }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        sc.stop();
    }
}