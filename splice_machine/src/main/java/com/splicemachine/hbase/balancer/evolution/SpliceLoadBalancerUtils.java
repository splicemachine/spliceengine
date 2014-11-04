package com.splicemachine.hbase.balancer.evolution;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionPlan;

public class SpliceLoadBalancerUtils {

    public static List<RegionPlan> createRegionMapping(Map<ServerName, List<HRegionInfo>> clusterState) {
    	List<RegionPlan> regionPlans = new ArrayList<RegionPlan>();    	
        for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
            for (HRegionInfo region : entry.getValue()) {
            	RegionPlan regionPlan = new RegionPlan(region,entry.getKey(),entry.getKey());
            	regionPlans.add(regionPlan);
            }
        }
        return regionPlans;
    }
}
