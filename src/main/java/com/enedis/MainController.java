package com.enedis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Collection;

@RestController
public class MainController {

    private Logger logger = LoggerFactory.getLogger(MainController.class);

    @RequestMapping("/createTable")
    public String createTable(){
        Configuration config = HBaseConfiguration.create();
        try {
            HBaseAdmin admin = new HBaseAdmin(config);
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("people"));
            // ... with two column families
            tableDescriptor.addFamily(new HColumnDescriptor("name"));
            tableDescriptor.addFamily(new HColumnDescriptor("contactinfo"));
            admin.createTable(tableDescriptor);
            return "OK";
        } catch (IOException e) {
            e.printStackTrace();
            return "KO";
        }
    }
    @RequestMapping("/listTableTest")
    public String listTable(){
        Configuration config = HBaseConfiguration.create();
        try {
            HBaseAdmin admin = new HBaseAdmin(config);
            HTable table = new HTable(config, "test");
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                String id = new String(result.getRow());
                logger.info("id = "+id);
            }

            return "OK";
        } catch (IOException e) {
            e.printStackTrace();
            return "KO";
        }
    }

    @RequestMapping("/admin")
    public String admin(){
        try {
            Connection connection = ConnectionFactory.createConnection();
            Admin admin = connection.getAdmin();
            ClusterStatus clusterStatus = admin.getClusterStatus();
            Collection<ServerName> servers = clusterStatus.getServers();
            for (ServerName server : servers) {
                logger.info("getting region load for "+server.getHostAndPort());
                Collection<RegionLoad> regionLoads = clusterStatus.getLoad(server).getRegionsLoad().values();
                regionLoads.forEach(regionLoad -> logger.info("load of "+regionLoad.getNameAsString()+" : "+regionLoad.getDataLocality()));
            }

            return "OK";
        } catch (IOException e) {
            e.printStackTrace();
            return "KO";
        }
    }
}
