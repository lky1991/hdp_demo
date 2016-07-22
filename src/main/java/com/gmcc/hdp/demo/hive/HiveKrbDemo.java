package com.gmcc.hdp.demo.hive;

import com.gmcc.hdp.demo.util.HDPSampleConfiguration;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * 基于kerberos进行hive的开发,在开发过程中留意在代码注释中提示的不同之处
 * Created by makun on 16/7/15.
 */
public class HiveKrbDemo {

    private Connection con = null;
    private ArrayList<String> hiveServerList = null;
    private String dataBaseName = null;
    private Statement statement = null;
    private Configuration conf = null;

    public HiveKrbDemo(String dataBaseName) {
        this.dataBaseName = dataBaseName;//数据库名称
        String hiveServerName = HDPSampleConfiguration.HIVE_SERVER_NAME;//加载hive server的配置文件

        hiveServerList = new ArrayList<String>();
        if (hiveServerName != null) {
            hiveServerList.addAll(Arrays.asList(hiveServerName.split(",")));
        }
    }


    /**
     * 建立数据库连接,kerberos场景下的主要不同处
     *
     * @param server
     * @return
     */
    public boolean connection(String server) {
        boolean flag = false;
        try {
            Class.forName(HDPSampleConfiguration.HIVE_DRIVER_NAME);
            /**
             * 对于使用kerberos连接时,与非kerberos不同地方:
             * 非kerberos连接:jdbc:hive2://hive2_host:10000/database;user=User;password=userPassword
             * kerberos连接:jdbc:hive2://hive2_host:10000/database;principal=hive/hive2_host@YOUR-REALM.COM
             * 详情可参考:http://www.cloudera.com/documentation/archive/cdh/4-x/4-2-0/CDH4-Security-Guide/cdh4sg_topic_9_1.html
             */
            String url = "jdbc:hive2://" + server + ":10000/" + dataBaseName + ";principal=" + HDPSampleConfiguration.HIVE_KERBEROS_PRINCIPAL;
            con = DriverManager.getConnection(url, "", "");

            if (null != con) {
                flag = true;
            }
            return flag;
        } catch (Exception e) {
            e.printStackTrace();
            return flag;
        }
    }

    /**
     * 连接Hive数据库
     *
     * @return
     */
    public boolean connHive() {
        boolean flag = false;
        for (String server : hiveServerList) {
            if (connection(server)) {
                flag = true;
                break;
            }
        }
        return flag;
    }

    /**
     * 关闭Hive数据库
     */
    public void closeResource() {
        try {
            if (null != statement) {
                statement.close();
            }

            if (null != con) {
                con.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     *
     * @param table
     */
    public boolean createHiveTable(String table) {
        if (null == con) {
            if (!connHive()) {
                System.out.println("Hive 数据库连接错误");
            }
        }
        boolean flag = false;

        try {
            if (statement == null) {
                statement = con.createStatement();
            }

            flag = statement.execute("create table if not exists " + table + "(LA_CODE String,BSCAD00 String,PROVINCE_NAME_CH String,CITY_NAME_CH String,TOWN_NAME_CH String,CELL_NAME_CN String) row format delimited  fields terminated by ',' STORED AS TEXTFILE");
            return flag;

        } catch (Exception e) {
            e.printStackTrace();
            return flag;
        }
    }

    /**
     * 查看数据库中的所有表
     */
    public void showDatabaseTables() {
        if (null == con) {
            if (!connHive()) {
                System.out.println("Hive 数据库连接错误");
            }
        }

        try {
            if (statement == null) {
                statement = con.createStatement();
            }

            ResultSet resultSet = null;
            String sql = "show tables ";
            resultSet = statement.executeQuery(sql);
            if (null != resultSet) {
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 查看表中所有数据
     *
     * @param table
     */
    public void searchHiveTable(String table) {

        if (null == con) {
            if (!connHive()) {
                System.out.println("Hive 数据库连接错误");
            }
        }

        try {
            if (statement == null) {
                statement = con.createStatement();
            }

            ResultSet resultSet = null;
            String sql = "select * from " + table + " where CITY_NAME_CH like '%广州市%'";
            resultSet = statement.executeQuery(sql);

            if (null != resultSet) {
                while (resultSet.next()) {
                    System.out.println(resultSet.getString(1) + " " + resultSet.getString(2) + " " + resultSet.getString(3) + " " + resultSet.getString(4) + " " + resultSet.getString(5) + " " + resultSet.getString(6));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 向表中加载数据
     *
     * @param table
     */
    public void insertHiveTable(String table) {
        if (null == con) {
            if (!connHive()) {
                System.out.println("Hive 数据库连接错误");
            }
        }

        try {
            if (statement == null) {
                statement = con.createStatement();
            }
            statement.execute("LOAD DATA INPATH '/tmp/2G_user_location_data.csv' OVERWRITE INTO TABLE " + table);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除表
     *
     * @param table
     */
    public boolean deleteHiveTable(String table) {
        if (null == con) {
            if (!connHive()) {
                System.out.println("Hive 数据库连接错误");
            }
        }
        boolean flag = false;

        try {
            if (statement == null) {
                statement = con.createStatement();
            }

            flag = statement.execute("drop table if exists " + table);
            return flag;
        } catch (Exception e) {
            e.printStackTrace();
            return flag;
        }
    }
}
