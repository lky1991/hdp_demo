package com.gmcc.hdp.demo.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by makun on 16/7/15.
 */
public class MRKrbDemo {
    /**
     * mapper类
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] array = line.split(",");
            if (array[3] != null) {
                word.set(array[3]);
                context.write(word, one);
            }
        }
    }

    /**
     * reduce 类
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    /**
     * 配置MapReduce任务
     *
     * @param input
     * @param output
     * @throws Exception
     */
    public void executeMR(String input, String output) throws Exception {
        Configuration conf = new Configuration();
        /**
         * 在启动kerberos的服务器上,如果对yarn进行了队列限制,在这里输入对应队列的最终
         * 叶子队列名称即可
         * 且提交的mr任务所在系统用户要与principle用户一致
         */
        conf.set("mapred.job.queue.name", "jim1");

        Job job1 = new Job(conf, "MRKrb_job");
        job1.setJarByClass(MRKrbDemo.class);
        job1.setMapperClass(TokenizerMapper.class);// 指定Map计算的类
        job1.setCombinerClass(IntSumReducer.class);// 合并的类
        job1.setReducerClass(IntSumReducer.class);// Reduce的类
        job1.setOutputKeyClass(Text.class);// 输出Key类型
        job1.setOutputValueClass(IntWritable.class);// 输出值类型

        FileInputFormat.addInputPath(job1, new Path(input));// 指定输入路径
        FileOutputFormat.setOutputPath(job1, new Path(output));// 指定输出路径

        System.exit(job1.waitForCompletion(true) ? 0 : 1);// 执行完MR任务后退出应用
    }
}
