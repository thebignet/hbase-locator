package com.enedis;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.stream.Collectors;

@Service
public class LocalityService {
    public Collection<RegionDataLocality> getRegionDataLocalities() {
        try (Connection connection = ConnectionFactory.createConnection()) {
            Admin admin = connection.getAdmin();
            ClusterStatus clusterStatus = admin.getClusterStatus();
            return clusterStatus.getServers().stream()
                    .flatMap(server -> {
                        Collection<RegionLoad> regionLoads = clusterStatus.getLoad(server).getRegionsLoad().values();
                        return regionLoads.stream()
                                .map(regionLoad -> new RegionDataLocality(regionLoad.getNameAsString(), regionLoad.getDataLocality()));
                    })
                    .sorted(Comparator.comparing(RegionDataLocality::getDataLocality))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }
}
