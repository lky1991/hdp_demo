#!/bin/sh
print_usage ()
{
    echo "Usage: sh run.sh COMMAND"
    echo "where COMMAND is one of the follows:"
    echo "-service HBase  -type   <createTb|delTb|put|get>              -tbName <创建和读取2G用户维表的表名>         -key <主键名称>           进行基于hbase的样例执行"
    echo "-service HDFS   -type   <write|read>                          -filepath <HDFS读写2G用户维表的文件路径>                               进行基于hdfs的样例执行"
    echo "-service MR     -input  <HDFS中2G用户维表的文件路径>          -output <存放统计结果的HDFS的文件路径>                                 进行基于mr的样例执行"
    echo "-service Kafka  -type   <producer|consumer>                   -topic    <生产或消费的topic>                                          进行基于kafka的样例执行"
    echo "-service Hive   -type   <createTb|deTb|insert|search>         -tbName   <Hive中的表名>                     -key<查询条件>            进行基于Hive的样例执行"
    exit 1
}

if [ $# = 0 ] || [ $1 = "help" ]; then
  print_usage
fi


CLASS=com.gmcc.hdp.demo.cli.RunningCli

JAVANAME=dt-gmcc-hdp-demo-1.0-SNAPSHOT-jar-with-dependencies.jar
params=$@
yarn jar $JAVANAME $CLASS $params
