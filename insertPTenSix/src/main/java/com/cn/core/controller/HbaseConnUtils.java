package com.cn.core.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HbaseConnUtils {
    private static final HbaseConnUtils INSTANCE = new HbaseConnUtils();
    private static Configuration configuration;
    private static Connection connection;

    private HbaseConnUtils() {
        try {
            if (configuration == null) {
                configuration = HBaseConfiguration.create();
                //System.setProperty("HADOOP_USER_NAME", "hbase");

                //configuration.set("hbase.zookeeper.quorum","192.168.56.211");
                configuration.set("hbase.zookeeper.quorum","127.0.0.1");
                configuration.set("hbase.zookeeper.property.clientPort", "2181");
                //configuration.set("hbase.zookeeper.znode.parent", "/hbase");
                //configuration.setInt("hbase.regionserver.handler.count", 100);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                System.out.println("get connection。。。。");
                connection = ConnectionFactory.createConnection(configuration);
            } catch (Exception e) {
                System.out.println("get connection。 error。。。");
                e.printStackTrace();
            }
        }
        return connection;
    }

    /**
     * 获取hbase连接
     *
     * @return
     */
    public static Connection getHbaseConn() {
        return INSTANCE.getConnection();
    }

    /**
     * 获取表
     *
     * @param tableName
     * @return
     */
    public static Table getTable(String tableName) throws IOException {
        System.out.println("get table。。。。");
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }

    /**
     * 关闭连接
     */
    public static void closeConn() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
