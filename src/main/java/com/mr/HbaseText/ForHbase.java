package com.mr.HbaseText;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by Administrator on 2018/01/30.
 */
public class ForHbase {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","192.168.174.134");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("TEXT"));
        HColumnDescriptor HCD1 = new HColumnDescriptor(Bytes.toBytes("fam1"));
        desc.addFamily(HCD1);
        admin.createTable(desc);
        admin.close();
        connection.close();
    }
}
