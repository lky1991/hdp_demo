package com.gmcc.hdp.demo.spark;

import com.gmcc.hdp.demo.util.HDPSampleConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 对ambari的日志文件统计每小时的产生量
 * Created by $wally on 2016/7/18
 */
public class SparkStreamingDemo {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingExample");

        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(30));
        JavaDStream<String> lines = jsc.textFileStream(HDPSampleConfiguration.SPARK_AMBARI_LOG_PATH);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                String word = null;
                if (!s.trim().isEmpty()) {
                    String[] res = s.split(" ");
                    if (res.length >= 3) {
                        word = res[1] + ":" + res[2].split(":")[0];
                    }
                }
                return Arrays.asList(word);
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> result = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        result.print();

        jsc.start();
        jsc.awaitTermination();
    }
}
