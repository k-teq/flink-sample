package com.kteq.flink;

import java.io.IOException;
import java.util.Set;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by mancini on 01/09/2018.
 *
 * naive hbase output format
 */


public class HBaseOutputFormat implements OutputFormat<Tuple3<String, Set<String>, Statistic>> {

    private static final long serialVersionUID = 1L;

    private transient Connection connection = null;
    private byte[] tableName;


    public HBaseOutputFormat(String tableName) {
        this.tableName = Bytes.toBytes(tableName);
    }


    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        connection = ConnectionFactory.createConnection();
    }

    @Override
    public void writeRecord(Tuple3<String, Set<String>, Statistic> record) throws IOException {

        try(Table table = connection.getTable(TableName.valueOf(tableName))) {

            for (String s : record.f1) {
                Put put = new Put(Bytes.toBytes( s + record.f0  )); //a row for each subscriber/key
                put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes(record.f0),
                        Bytes.toBytes( record.f2.toString() ));
                table.put(put);
            }
        }
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }

}


