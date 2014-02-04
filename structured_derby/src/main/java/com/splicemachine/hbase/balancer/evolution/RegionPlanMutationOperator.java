package com.splicemachine.hbase.balancer.evolution;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.uncommons.watchmaker.framework.EvolutionaryOperator;

public class RegionPlanMutationOperator implements EvolutionaryOperator<RegionPlan> {
	public List<ServerName> servers;
	
	public RegionPlanMutationOperator(List<ServerName> servers) {
		this.servers = servers;
	}
	
	@Override
	public List<RegionPlan> apply(List<RegionPlan> selectedCandidates,Random rng) {
		List<RegionPlan> appliedPlans = new ArrayList<RegionPlan>(selectedCandidates.size());
		for (RegionPlan regionPlan: selectedCandidates) {
			if (rng.nextBoolean())
				regionPlan.setDestination(servers.get(servers.size()-1));
			appliedPlans.add(regionPlan);
		}
		return appliedPlans;
	}

}
