#!/bin/sh
print_usage ()
{
    echo "Usage: sh run.sh COMMAND"
    echo "where COMMAND is one of the follows:"
    echo "-service HBase -type <createTb|delTb|put|get>    -tbName <创建和读取2G用户维表的表名>      -key <HBase rowkey>   -krb <true|false,true表示在kerberos环境下执行>  -princ <kerberos下的principle名称>   -keytab <kerberos下的keytable路径>  进行基于hbase的样例执行"
    echo "-service HDFS  -type <write|read>                -filepath <HDFS读写2G用户维表的文件路径>  -krb <true|false,true表示在kerberos环境下执行>  -princ <kerberos下的principle名称>   -keytab <kerberos下的keytable路径>                       进行基于hdfs的样例执行"
    echo "-service MR    -input <HDFS中2G用户维表的文件路径>  -output <存放统计结果的HDFS的文件路径>    -krb <true|false,true表示在kerberos环境下执行>  -princ <kerberos下的principle名称>   -keytab <kerberos下的keytable路径>                       进行基于mr的样例执行"
    echo "-service HIVE  -type <create|read|insert>        -dbName <数据库>   -tbName <数据库表>    -krb <true|false,true表示在kerberos环境下执行>  -princ <kerberos下的principle名称>   -keytab <kerberos下的keytable路径>                       进行基于hive的样例执行"
    echo "-service Kafka -type <producer|consumer>  -topic <主题>      -krb <true|false,true表示在kerberos环境下执行>  -princ <kerberos下的principle名称>   -keytab <kerberos下的keytable路径>                                                     进行基于kafka的样例执行"

    exit 1
}

if [ $# = 0 ] || [ $1 = "help" ]; then
  print_usage
fi


CLASS=com.gmcc.hdp.demo.cli.RunningKrbCli

JARNAME=dt-gmcc-hdp-demo-1.0-SNAPSHOT-jar-with-dependencies.jar
params=$@
yarn jar $JARNAME $CLASS $params
