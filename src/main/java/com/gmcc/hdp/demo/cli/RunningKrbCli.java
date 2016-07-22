package com.gmcc.hdp.demo.cli;


import com.gmcc.hdp.demo.hbase.HBaseDemo;
import com.gmcc.hdp.demo.hbase.HBaseKrbDemo;
import com.gmcc.hdp.demo.hdfs.HDFSDemo;
import com.gmcc.hdp.demo.hive.HiveDemo;
import com.gmcc.hdp.demo.hive.HiveKrbDemo;
import com.gmcc.hdp.demo.kafka.KafkaDemo;
import com.gmcc.hdp.demo.mr.MRKrbDemo;
import com.gmcc.hdp.demo.util.HDPSampleConfiguration;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * 在使用kerberos场景下提交任务
 * Created by makun on 16/7/15.
 */
public class RunningKrbCli {
    private Options options = new Options();
    private CommandLine commandLine = null;

    public RunningKrbCli(String[] args) {
        initOpts();
        try {
            // 解析参数
            CommandLineParser parser = new PosixParser();
            commandLine = parser.parse(options, args);

            //启动kerberos登录监控线程
            String princ = commandLine.getOptionValue("princ");
            String keytab = commandLine.getOptionValue("keytab");
            new LoginKrbThread(princ,keytab);

            //提交服务
            String service = commandLine.getOptionValue("service");
            if (service.equals("HBase")) {
                hbaseLearnSampleCli();
            } else if (service.equals("HDFS")) {
                hdfsLearnSampleCli();
            } else if (service.equals("MR")) {
                mrLearnSampleCli();
            } else if (service.equals("HIVE")) {
                hiveLearnSampleCli();
            }
        } catch (ParseException e) {
            System.out.println(e);
        }

        // ** 启动kerberos动态登录后,需显式的主动退出服务,否则监听将会一直持续 **
        System.exit(0);
    }

    public void initOpts() {
        options.addOption("service", true, "执行服务");
        options.addOption("type", true, "处理类型");
        options.addOption("tbName", true, "用户HBase表的创建");
        options.addOption("key", true, "用户HBase key的搜索");
        options.addOption("filepath", true, "用户HDFS文件的读写使用");
        options.addOption("input", true, "HDFS中2G用户维表的文件路径");
        options.addOption("output", true, "存放统计结果的HDFS的文件路径");
        options.addOption("krb", true, "是否在kerberos环境下执行");
        options.addOption("dbName", true, "数据库名称");
        options.addOption("princ", true, "kerberos对应的principle完整名称");
        options.addOption("keytab", true, "kerberos对应的keytab路径");
        options.addOption("topic", true, "kafka中的topic");
    }


    public void hbaseLearnSampleCli() {
        String type = commandLine.getOptionValue("type");
        String kerberos = commandLine.getOptionValue("krb");
        if (kerberos.equals("true")) {

            HBaseKrbDemo hBaseKrbDemo=new HBaseKrbDemo();

            if (type.equals("createTb")) {
                String table = commandLine.getOptionValue("tbName");

                if (null == table) {
                    System.out.println("请输入要创建的表名");
                } else {
                    String[] array = {HDPSampleConfiguration.HBASE_COLUMNFAMILY_NAME};
                    hBaseKrbDemo.create(table, array);
                }

            } else if (type.equals("delTb")) {
                String table = commandLine.getOptionValue("tbName");

                if (table == null) {
                    System.out.println("请输入要删除的表名");
                } else {
                    try {
                        hBaseKrbDemo.delete(table);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } else if (type.equals("put")) {
                String table = commandLine.getOptionValue("tbName");

                if (null == table) {
                    System.out.println("请输入要插入的表名");
                } else {
                    hBaseKrbDemo.initTable(table);
                }

            } else if (type.equals("get")) {
                String key = commandLine.getOptionValue("key");
                String table = commandLine.getOptionValue("tbName");
                if (key == null || table == null) {
                    System.out.println("请输入要查询的表名和主键名");
                } else {
                    hBaseKrbDemo.get(table, key);
                }
            } else {
                System.out.println("请输入要执行的操作类型:createTb,delTb,put,get");
            }
        }else{
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
    }

    public void hdfsLearnSampleCli() {
        String type = commandLine.getOptionValue("type");
        String kerberos = commandLine.getOptionValue("krb");
        HDFSDemo hdfsLearnSample = null;
        hdfsLearnSample = new HDFSDemo();

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

        MRKrbDemo mrLearnSample = new MRKrbDemo();

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

    public void hiveLearnSampleCli() {
        String type = commandLine.getOptionValue("type");
        String dbName = commandLine.getOptionValue("dbName");
        String table = commandLine.getOptionValue("tbName");
        String krb = commandLine.getOptionValue("krb");
        if (dbName==null) {
            dbName = "default";
        }
        if (table == null) {
            System.out.println("请输入连接数据库的表名");
            return;
        }

        if (krb.equals("true")) {
           HiveKrbDemo hiveKrbDemo = new HiveKrbDemo(dbName);
            if (type.equals("create")) {
               hiveKrbDemo.createHiveTable(table);
            }
            else if (type.equals("read")) {
               hiveKrbDemo.searchHiveTable(table);
            }
            else if (type.equals("insert")) {
               hiveKrbDemo.insertHiveTable(table);
            }
        }
        else {
            HiveDemo hiveLearnExample = new HiveDemo();
            if (type.equals("create")) {
                hiveLearnExample.createHiveTable(table);
            }
            else if (type.equals("read")) {
                hiveLearnExample.searchHiveTable(table,"");
            }
            else if (type.equals("insert")) {
                hiveLearnExample.insertHiveTable(table);
            }
        }
    }

    /**
     * kerberos登录监听线程
     */
     class LoginKrbThread implements Runnable {
        Thread t;
        long sleeptime = 1*60*1000;
        String principle = null;
        String keytabPath = null;

        public LoginKrbThread(String principle,String path) {
            this.principle = principle;
            this.keytabPath = path;
            loginKrb();
            t = new Thread(this,"login kerberos thread");
            t.start();
        }

        public void loginKrb() {
            /**
             * kinit kerberos用户,使用principle&keytab登录用户
             */
            try {
                Configuration conf = new Configuration();
                conf.set("hadoop.security.authentication", "Kerberos");
                UserGroupInformation.setConfiguration(conf);
                UserGroupInformation.loginUserFromKeytab(this.principle, this.keytabPath);
            }catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        public void run() {
            try {
                while (true) {
                    Thread.sleep(this.sleeptime);
                    loginKrb();
                }
            }catch (InterruptedException ex) {
                ex.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        RunningKrbCli runningCli = new RunningKrbCli(args);
    }
}
