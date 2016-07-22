package com.gmcc.hdp.demo.spark;

import com.gmcc.hdp.demo.util.HDPSampleConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 首先，将本地的（2G小区的维表.csv）数据写入到HDFS中某一个具体的文件中，然后开发Spark程序对广东省每个市的2G小区的数量进行统计。
 * Created by $wally on 2016/7/17.
 */
public class SparkDemo {

    /**
     * 提取输入文件中的城市名
     */
    private static class SplitFunction implements FlatMapFunction<String, String> {

        public Iterable<String> call(String s) throws Exception {
            String word = null;
            if (!s.trim().isEmpty()) {
                String[] array = s.split(",");
                if (array.length >= 4) {
                    if (array[3] != null) {
                        word = array[3];
                    }
                }
            }
            return Arrays.asList(word);
        }
    }

    /**
     * 将String转换成tuple
     */
    private static class PairsFunction implements PairFunction<String, String, Integer> {

        public Tuple2<String, Integer> call(String s) throws Exception {
            return new Tuple2<String, Integer>(s, 1);
        }
    }

    /**
     * 根据key值对tuple中的value进行汇总
     */
    private static class ReduceFunction implements Function2<Integer, Integer, Integer> {
        public Integer call(Integer a, Integer b) throws Exception {
            return a + b;
        }
    }

    /**
     * 执行spark job
     * @param sc
     */
    public void cityCount(JavaSparkContext sc) {

        String inputPath = HDPSampleConfiguration.SPARK_INPUT_PATH;
        String outputPath = HDPSampleConfiguration.SPARK_OUTPUT_PATH;

        JavaRDD<String> data = sc.textFile(inputPath).cache();

        JavaRDD<String> words = data.flatMap(new SplitFunction());
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairsFunction());
        JavaPairRDD<String, Integer> result = pairs.reduceByKey(new ReduceFunction());

        List<Tuple2<String, Integer>> count = result.collect();

        for (Tuple2<String, Integer> item : count) {
            System.out.println(item._1() + ": " + item._2());
        }

        //设置输出结果保存在一个文件中
        result.repartition(1).saveAsTextFile(outputPath);
    }


    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setAppName("SparkExample");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        SparkDemo sparkLearnSample = new SparkDemo();
        sparkLearnSample.cityCount(sparkContext);
        sparkContext.stop();
    }
}
