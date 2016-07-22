
package com.gmcc.hdp.demo.hive;

import com.gmcc.hdp.demo.util.HDPSampleConfiguration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;


/**
 * 利用2G小区的维表数据在Hive上实现了表的创建、删除，表中数据的查询以及向表中插入数据
 * Created by $wally on 2016/7/12.
 */
public class HiveDemo {

    private Connection con = null;
    private ArrayList<String> hiveServerList = null;
    private Statement statement = null;

    public HiveDemo() {
        String hiveServerName = HDPSampleConfiguration.HIVE_SERVER_NAME;//加载hive server的配置文件

        hiveServerList = new ArrayList<String>();
        if (hiveServerName != null) {
            hiveServerList.addAll(Arrays.asList(hiveServerName.split(",")));
        }
    }

    /**
     * @param server HiveServer地址
     * @return
     */

    public boolean connection(String server) {
        boolean flag = false;
        try {
            //加载连接Hive数据库的驱动
            Class.forName(HDPSampleConfiguration.HIVE_DRIVER_NAME);

            //连接HiveServer
            con = DriverManager.getConnection("jdbc:hive2://" + server + ":10000/" + HDPSampleConfiguration.HIVE_DATABASE_NAME, HDPSampleConfiguration.HIVE_PLATFORM_USERNAME, HDPSampleConfiguration.HIVE_PLATFORM_PASSWORD);

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
    public void createHiveTable(String table) {
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

            if (!flag) {
                System.err.println("成功创建表：" + table);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 从表中查询数据
     * @param table Hive中的表名
     * @param key   广东省的市名
     */
    public void searchHiveTable(String table, String key) {
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
            String sql = "select * from " + table + " where CITY_NAME_CH like '%" + key + "%'";
            resultSet = statement.executeQuery(sql);

            System.err.println("-----输出查询结果----");
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

            String sql = "LOAD DATA  INPATH '/tmp/2G_user_location_data.csv' OVERWRITE INTO TABLE " + table;
            statement.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除表
     *
     * @param table
     */
    public void deleteHiveTable(String table) {
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
            if (!flag) {
                System.err.println("成功删除表：" + table);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
