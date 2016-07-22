package com.gmcc.hdp.demo.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;

/**
 * HA和非HA下相同
 * HDFS文件系统基本操作类（文件读、文件写）
 * Created by $wally on 2016/7/5.
 */
public class HDFSHADemo {
    private Configuration conf = null;

    public HDFSHADemo() {
        conf = new Configuration();
    }

    /**
     * 从HDFS的文件中读数据
     *
     * @param pathName
     */
    public void readHDFSFile(String pathName) {
        FSDataInputStream hdfsInStream = null;
        FileSystem fs = null;

        try {
            fs = FileSystem.get(conf);
            Path path = new Path(pathName);

            if (fs.exists(path)) {
                hdfsInStream = fs.open(path);

                byte[] ioBuffer = new byte[2048];
                int len = hdfsInStream.read(ioBuffer);
                while (len != -1) {
                    System.out.write(ioBuffer, 0, len);
                    len = hdfsInStream.read(ioBuffer);
                }
            }
            hdfsInStream.close();
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != hdfsInStream) {
                    hdfsInStream.close();
                }
                if (fs != null) {
                    fs.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 向HDFS的文件中写数据
     *
     * @param localName
     * @param hdfsName
     */
    public void upFileToHDFSFromLocal(String localName, String hdfsName) {
        FileSystem fs = null;
        BufferedReader reader = null;
        FSDataOutputStream hdfsOutStream = null;
        try {
            reader = new BufferedReader(new InputStreamReader(HDFSHADemo.class.getClassLoader().getResourceAsStream(localName)));
            fs = FileSystem.get(URI.create(hdfsName), conf);
            Path path = new Path(hdfsName);
            if (fs.exists(path)) {
                System.out.println("目标文件已经存在，请删除后再操作");
                return;
            }
            hdfsOutStream = fs.create(path);

            String line = null;
            int index = 0;
            while ((line = reader.readLine()) != null) {
                if (index % 1000 == 0) {
                    System.out.print(".");
                }
                index++;
                hdfsOutStream.writeUTF(new String(line.getBytes(), "utf-8"));
                hdfsOutStream.write("\n".getBytes(), 0, "\n".length());

            }
            reader.close();
            hdfsOutStream.close();
            fs.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != reader) {
                    reader.close();
                }

                if (null != hdfsOutStream) {
                    hdfsOutStream.close();
                }

                if (null != fs) {
                    fs.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
