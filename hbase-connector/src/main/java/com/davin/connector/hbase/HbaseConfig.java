package com.davin.connector.hbase;

import java.io.IOException;

import javax.annotation.PostConstruct;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HbaseConfig {
    private static final Logger logger = LoggerFactory.getLogger(HbaseConfig.class);
    private Connection conn = null;
    private org.apache.hadoop.conf.Configuration conf;
    @Value("${spring.connector.hbase.zkAddress}")
    private String zkAddress;
    @Value("${spring.connector.hbase.zkPort}")
    private String zkPort;
    @Value("${spring.connector.hbase.zkDirectory}")
    private String zkDirectory;
    
    @PostConstruct
    public void init() {
        conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM, zkAddress);
        conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.parseInt(zkPort));
        conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, zkDirectory);
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            logger.info("init hbase connector error", e);
            e.printStackTrace();
        }
    }

    public Connection getConn() {
        return conn;
    }

    public void setConn(Connection conn) {
        this.conn = conn;
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

     
    public String getZkPort() {
        return zkPort;
    }

    public void setZkPort(String zkPort) {
        this.zkPort = zkPort;
    }

    public String getZkDirectory() {
        return zkDirectory;
    }

    public void setZkDirectory(String zkDirectory) {
        this.zkDirectory = zkDirectory;
    }

    public org.apache.hadoop.conf.Configuration getConf() {
        return conf;
    }

    public void setConf(org.apache.hadoop.conf.Configuration conf) {
        this.conf = conf;
    }
}
