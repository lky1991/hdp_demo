package com.gmcc.hdp.demo.hbase;

import com.gmcc.hdp.demo.util.CsvFileUtil;
import com.gmcc.hdp.demo.util.HDPSampleConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HBase数据库的基本操作类（创建表，删除表，初始化表数据，根据主键查询表中数据）
 * Created by $wally on 2016/7/4.
 */
public class HBaseDemo {
    public Configuration cfg = null;

    public HBaseDemo() {
        Configuration hbase_config = new Configuration();

        // 添加HBase依赖的Zookeeper的通信端口
        hbase_config.set("hbase.zookeeper.property.clientPort", "2181");

        // 添加HBase依赖的Zookeeper服务器地址
        hbase_config.set("hbase.zookeeper.quorum", HDPSampleConfiguration.HBASE_ZOOKEEPER_QUORUM_LIST);

        // 添加HBase在其依赖的Zookeeper上的目录
        hbase_config.set("zookeeper.znode.parent", "/hbase-unsecure");
        cfg = HBaseConfiguration.create(hbase_config);
    }


    /**
     * 创建表
     * @param tableName
     * @param columnFamily
     * @throws Exception
     */
    public void create(String tableName, String[] columnFamily) {
        HBaseAdmin admin = null;

        try {
            admin = new HBaseAdmin(cfg);

            if (admin.tableExists(tableName)) {
                System.err.println("该表已经存在");
            } else {
                HTableDescriptor tableDesc = new HTableDescriptor(tableName.valueOf(tableName));

                for (String item : columnFamily) {
                    tableDesc.addFamily(new HColumnDescriptor(item));
                }

                admin.createTable(tableDesc);
                System.out.println("创建表成功");
            }

            admin.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeHBaseAdmin(admin);
        }
    }

    /**
     * 在表中插入一行数据
     * @param tableName
     * @param row
     * @param columnFamily
     * @param column
     * @param data
     * @throws Exception
     */
    public void put(String tableName, String row, String columnFamily, String[] column, String[] data) {
        HTable table = null;

        try {
            table = new HTable(cfg, tableName);

            Put p1 = new Put(Bytes.toBytes(row));
            for (int i = 0; i < column.length; ++i) {
                p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column[i]), Bytes.toBytes(data[i]));
            }
            table.put(p1);
            System.out.println("成功添加一行数据!");

            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeHTable(table);
        }
    }

    /**
     * 根据主键查询表中的信息
     * @param tableName
     * @param row
     * @throws Exception
     */
    public void get(String tableName, String row) {
        HTable table = null;

        try {
            table = new HTable(cfg, tableName);

            Get get = new Get(Bytes.toBytes(row));
            Result result = table.get(get);

            System.out.println("该行数据为： " + String.valueOf(result));
            for (KeyValue keyValue : result.raw()) {
                System.out.println("列簇：" + new String(keyValue.getRow(), "utf-8") + " " + new String(keyValue.getQualifier(), "utf-8") + "====值:" + new String(keyValue.getValue(), "utf-8"));
            }

            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeHTable(table);
        }
    }


    /**
     * 删除表中数据
     * @param tableName
     * @return
     * @throws IOException
     */
    public boolean delete(String tableName) throws IOException {
        HBaseAdmin admin = null;

        try {
            admin = new HBaseAdmin(cfg);

            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }

            System.err.println("------" + tableName + "-------表删除成功");

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            closeHBaseAdmin(admin);
        }
    }

    /**
     * 判断该表是否已经创建
     *
     * @param tableName
     * @return
     */
    public boolean isTableExists(String tableName) {

        HBaseAdmin admin = null;
        boolean flag = false;

        try {
            admin = new HBaseAdmin(cfg);

            if (admin.tableExists(tableName)) {
                flag = true;
            }

            return flag;
        } catch (Exception e) {
            e.printStackTrace();
            return flag;
        } finally {
            closeHBaseAdmin(admin);
        }

    }

    /**
     * 关闭HTable对象
     * @param table
     */
    public void closeHTable(HTable table) {
        try {
            if (table != null) {
                table.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭HBaseAdmin对象
     * @param admin
     */
    public void closeHBaseAdmin(HBaseAdmin admin) {
        try {
            if (admin != null) {
                admin.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 初始化表数据
     * @param tableName
     */
    public void initTable(String tableName) {
        HTable table = null;

        try {
            table = new HTable(cfg, tableName);

            if (!isTableExists(tableName)) {
                System.err.println("------该表不存在----");
                return;
            }

            CsvFileUtil cvs = new CsvFileUtil(HDPSampleConfiguration.LOCAL_2G_USER_FILE);
            String fieldArray[] = cvs.readLine().substring(1).split(",");//获取列名
            String tempData = cvs.readLine();

            Integer count = 1;
            while (tempData != null) {
                String[] res = tempData.split(",");

                Put p1 = new Put(Bytes.toBytes("row" + count));
                for (int i = 0; i < res.length; ++i) {
                    p1.add(Bytes.toBytes(HDPSampleConfiguration.HBASE_COLUMNFAMILY_NAME), Bytes.toBytes(fieldArray[i]), Bytes.toBytes(res[i]));
                }
                table.put(p1);

                System.err.println("第" + count + "行数据添加成功");
                count++;
                tempData = cvs.readLine();
            }
            table.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeHTable(table);
        }
    }
}
