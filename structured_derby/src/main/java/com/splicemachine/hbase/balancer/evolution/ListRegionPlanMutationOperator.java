package com.splicemachine.hbase.balancer.evolution;

import java.util.List;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.uncommons.watchmaker.framework.operators.ListOperator;

public class ListRegionPlanMutationOperator extends ListOperator<RegionPlan> {

	public ListRegionPlanMutationOperator(List<ServerName> servers) {
		super(new RegionPlanMutationOperator(servers));
	}

}
