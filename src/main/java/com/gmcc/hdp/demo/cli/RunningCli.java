package com.gmcc.hdp.demo.cli;

import com.gmcc.hdp.demo.hbase.HBaseDemo;

import com.gmcc.hdp.demo.hdfs.HDFSDemo;
import com.gmcc.hdp.demo.hive.HiveDemo;
import com.gmcc.hdp.demo.kafka.KafkaDemo;
import com.gmcc.hdp.demo.mr.MRDemo;
import com.gmcc.hdp.demo.util.HDPSampleConfiguration;
import org.apache.commons.cli.*;


/**
 * cli类,用于解析输入的参数,执行相关的功能
 * Created by makun on 16/7/8.
 */
public class RunningCli {
    private Options options = new Options();
    private CommandLine commandLine = null;

    public RunningCli(String[] args) {
        initOpts();
        try {
            // 解析参数
            CommandLineParser parser = new PosixParser();
            commandLine = parser.parse(options, args);

            String service = commandLine.getOptionValue("service");
            if (service.equals("HBase")) {
                hbaseLearnSampleCli();
            } else if (service.equals("HDFS")) {
                hdfsLearnSampleCli();
            } else if (service.equals("MR")) {
                mrLearnSampleCli();
            } else if (service.equals("Kafka")) {
                kafkaLearnSampleCli();
            } else if (service.equals("Hive")) {
                hiveLearnSampleCli();
            }

        } catch (ParseException e) {
            System.out.println(e);
        }
    }

    public void initOpts() {
        options.addOption("service", true, "执行服务");
        options.addOption("type", true, "处理类型");
        options.addOption("tbName", true, "用户HBase表的创建");
        options.addOption("key", true, "用户HBase key的搜索");
        options.addOption("filepath", true, "用户HDFS文件的读写使用");
        options.addOption("input", true, "HDFS中2G用户维表的文件路径");
        options.addOption("output", true, "存放统计结果的HDFS的文件路径");
        options.addOption("topic", true, "Kafka队列中的主题名称");

    }

    public void hbaseLearnSampleCli() {
        String type = commandLine.getOptionValue("type");
        HBaseDemo hBaseLearnSample = new HBaseDemo();
        if (type.equals("createTb")) {
            String table = commandLine.getOptionValue("tbName");

            if (null == table) {
                System.out.println("请输入要创建的表名");
            } else {
                String[] array = {HDPSampleConfiguration.HBASE_COLUMNFAMILY_NAME};
                hBaseLearnSample.create(table, array);
            }

        } else if (type.equals("delTb")) {
            String table = commandLine.getOptionValue("tbName");

            if (table == null) {
                System.out.println("请输入要删除的表名");
            } else {
                try {
                    hBaseLearnSample.delete(table);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else if (type.equals("put")) {
            String table = commandLine.getOptionValue("tbName");

            if (null == table) {
                System.out.println("请输入要插入的表名");
            } else {
                hBaseLearnSample.initTable(table);
            }

        } else if (type.equals("get")) {
            String key = commandLine.getOptionValue("key");
            String table = commandLine.getOptionValue("tbName");
            if (key == null || table == null) {
                System.out.println("请输入要查询的表名和主键名");
            } else {
                hBaseLearnSample.get(table, key);
            }
        } else {
            System.out.println("请输入要执行的操作类型:createTb,delTb,put,get");
        }
    }

    public void hdfsLearnSampleCli() {
        String type = commandLine.getOptionValue("type");
        HDFSDemo hdfsLearnSample = new HDFSDemo();

        if (type.equals("write")) {
            String path = commandLine.getOptionValue("filepath");

            if (null == path) {
                System.out.println("请输入待写入的hdfs的文件路径");
            } else {
                hdfsLearnSample.upFileToHDFSFromLocal(HDPSampleConfiguration.LOCAL_2G_USER_FILE, path);
            }
        } else if (type.equals("read")) {
            String path = commandLine.getOptionValue("filepath");

            if (null == path) {
                System.out.println("请输入待读的hdfs的文件路径");
            } else {
                hdfsLearnSample.readHDFSFile(path);
            }
        }
    }

    public void mrLearnSampleCli() {
        String inputPath = commandLine.getOptionValue("input");
        String outputPath = commandLine.getOptionValue("output");

        MRDemo mrLearnSample = new MRDemo();

        if (null == inputPath || null == outputPath) {
            System.out.println("请输入待处理数据的输入路径和结果输出路径");
        } else {
            try {
                mrLearnSample.executeMR(inputPath, outputPath);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void kafkaLearnSampleCli() {
        KafkaDemo kafkaLearnSample = new KafkaDemo();
        String type = commandLine.getOptionValue("type");

        if (type.equals("producer")) {
            String topic = commandLine.getOptionValue("topic");
            if (null == topic) {
                System.out.println("请输入kafka topic 名称");
            } else {
                kafkaLearnSample.producerData(topic);
            }
        } else if (type.equals("consumer")) {
            String topic = commandLine.getOptionValue("topic");
            if (null == topic) {
                System.out.println("请输入kafka topic 名称");
            } else {
                kafkaLearnSample.consumerData(topic);
            }
        }
    }

    public void hiveLearnSampleCli() {
        HiveDemo hiveLearnSample = new  HiveDemo ();
        String type = commandLine.getOptionValue("type");

        if (type.equals("createTb")) {
            String table = commandLine.getOptionValue("tbName");
            if (null == table) {
                System.out.println("请输入待创建的table名称");
            } else {
                hiveLearnSample.createHiveTable(table);
            }
        } else if (type.equals("deTb")) {
            String table = commandLine.getOptionValue("tbName");
            if (null == table) {
                System.out.println("请输入待删除的table名称");
            } else {
                hiveLearnSample.deleteHiveTable(table);
            }
        } else if (type.equals("insert")) {
            String table = commandLine.getOptionValue("tbName");
            if (null == table) {
                System.out.println("请输入待插入的table名称");
            } else {
                hiveLearnSample.insertHiveTable(table);
            }
        } else if (type.equals("search")) {
            String table = commandLine.getOptionValue("tbName");
            String key = commandLine.getOptionValue("key");
            if (null == table || null == key) {
                System.out.println("请输入待查询的table名称和查询条件");
            } else {
                hiveLearnSample.searchHiveTable(table, key);
            }
        }

    }

    public static void main(String[] args) {
        RunningCli runningCli = new RunningCli(args);
    }
}
