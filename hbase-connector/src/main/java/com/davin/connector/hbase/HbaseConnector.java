package com.davin.connector.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
 
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.Export;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.jcraft.jsch.Logger;

@Service
public class HbaseConnector {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(HbaseConnector.class);
    @Autowired
    private HbaseConfig hbaseConfig;
    
    public void createTable(String tableName, String[] columnNames) throws IOException {
        Connection conn = hbaseConfig.getConn();
        Admin admin = conn.getAdmin();
        TableName table = TableName.valueOf(tableName);
        if (admin.tableExists(table)) {
            return;
        }
        List<ColumnFamilyDescriptor> families = getFamilys(columnNames);
        TableDescriptor t = TableDescriptorBuilder.newBuilder(table)
                .setCoprocessor(Export.class.getName())   
                .setColumnFamilies(families)
                .build();
        admin.createTable(t);
    }
    
    public void writeData(String tableName, List<Put> puts) {
        Connection conn = hbaseConfig.getConn();
        try (Table table = conn.getTable(TableName.valueOf(tableName))) {
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void writeData(String tableName, Put put) {
        Connection conn = hbaseConfig.getConn();
        try (Table table = conn.getTable(TableName.valueOf(tableName))) {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private List<ColumnFamilyDescriptor> getFamilys(String[] columnNames) {
        List<ColumnFamilyDescriptor> families = new ArrayList<>();
        for (String columnName: columnNames) {
            families.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnName)).build());
        }
        return families;
    }
    
    public void readData(String tableName, String[] columnNames, Integer startId, Integer endId) {
        List<ColumnFamilyDescriptor> families = getFamilys(columnNames);
        List<Put> puts = new ArrayList<>(10000);
        for (int row = startId; row != endId; ++row) {
          byte[] bs = Bytes.toBytes("documentname" + String.valueOf(row));
          Put put = new Put(bs);
          put.addColumn(families.get(0).getName(), bs, Bytes.toBytes("成长心得及基础知识框架_20190512_喜乐.pptx"));
          put.addColumn(families.get(1).getName(), bs, Bytes.toBytes("hbase:" + String.valueOf(row)));
          writeData(tableName, put);
          if (puts.size() >= 10000) {
              logger.info("max row is:" + endId + " current row is:" + row);
              writeData(tableName, puts);
              puts.clear();
          }
          puts.add(put);
        } 
    }
    
    public List<Put> readData(String[] columnNames, Integer startId, Integer endId) {
        List<Put> puts = new ArrayList<>(endId - startId);
        List<ColumnFamilyDescriptor> families = getFamilys(columnNames);
        for (int row = startId; row != endId; ++row) {
          byte[] bs = Bytes.toBytes("documentname" + String.valueOf(row));
          Put put = new Put(bs);
          put.addColumn(families.get(0).getName(), bs, Bytes.toBytes("成长心得及基础知识框架_20190512_喜乐.pptx"));
          put.addColumn(families.get(1).getName(), bs, Bytes.toBytes("hbase:" + String.valueOf(row)));
          puts.add(put);          
        }
        return puts;
    }
    

    public void scanTable(String tableName) throws IOException {
        Connection conn = hbaseConfig.getConn();
        TableName t = TableName.valueOf(tableName);
//        Admin admin = conn.getAdmin();
//        admin.disableTable(table);
//        TableDescriptor descriptor = admin.
//        String coprocessorClass = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
//        if (! descriptor.hasCoprocessor(coprocessorClass)) {
//            descriptor.
//        }
//        admin.modifyTable(name, descriptor);
//        admin.enableTable(name);
        Table table = conn.getTable(t);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        
        try {
            int rowId = 0;
            while (scanner.next() != null) {
                rowId ++;
            }
            System.out.println(rowId);
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }
    
    public void selectOneRow(String tableName, String rowKey, String[] columnNames) throws IOException {
        List<ColumnFamilyDescriptor> families = getFamilys(columnNames);
        Connection conn = hbaseConfig.getConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        Result result = table.get(get);
        System.out.println(Bytes.toString(result.getValue(families.get(0).getName(), Bytes.toBytes(rowKey))));
        System.out.println(Bytes.toString(result.getValue(families.get(1).getName(), Bytes.toBytes(rowKey))));
//        for (Cell cell : result.rawCells()) {
//            String value = Bytes.toString(cell.getFamilyArray());
//            System.out.println(value);
//        }
    }
}
