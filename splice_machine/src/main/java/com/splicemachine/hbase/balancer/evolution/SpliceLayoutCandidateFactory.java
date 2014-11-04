package com.splicemachine.hbase.balancer.evolution;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.uncommons.watchmaker.framework.factories.AbstractCandidateFactory;
import com.google.common.collect.Lists;

public class SpliceLayoutCandidateFactory extends AbstractCandidateFactory<List<RegionPlan>> {
	private Map<ServerName,List<HRegionInfo>> clusterState;
	public SpliceLayoutCandidateFactory(Map<ServerName, List<HRegionInfo>> clusterState) {
		this.clusterState = clusterState;
	}
	
	@Override
	public List<RegionPlan> generateRandomCandidate(Random rng) {
	  	List<RegionPlan> regionPlans = new ArrayList<RegionPlan>();    
	  	List<ServerName> servers = Lists.newArrayList(clusterState.keySet());
	  	int iServers = servers.size();
        for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
            for (HRegionInfo region : entry.getValue()) {
            	RegionPlan regionPlan = new RegionPlan(region,entry.getKey(),servers.get(rng.nextInt(iServers)));
            	regionPlans.add(regionPlan);
            }
        }
        return regionPlans;
	}
}
