package com.splicemachine.hbase.balancer.evolution;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.uncommons.maths.random.MersenneTwisterRNG;
import org.uncommons.watchmaker.framework.EvolutionObserver;
import org.uncommons.watchmaker.framework.GenerationalEvolutionEngine;
import org.uncommons.watchmaker.framework.PopulationData;
import org.uncommons.watchmaker.framework.selection.RouletteWheelSelection;
import org.uncommons.watchmaker.framework.termination.ElapsedTime;

public class SpliceLayoutGenerationalEvolutionEngine extends GenerationalEvolutionEngine<List<RegionPlan>> {
	
	public SpliceLayoutGenerationalEvolutionEngine(Map<ServerName, List<HRegionInfo>> clusterState) {
		super(null,null,SpliceLayoutFitness.create(),
				new RouletteWheelSelection(),new MersenneTwisterRNG());		
	}

	public void run() {
		addEvolutionObserver(new EvolutionObserver<List<RegionPlan>>() {
					@Override
					public void populationUpdate(PopulationData<? extends List<RegionPlan>> data) {
						System.out.println("Updating");
						// TODO Auto-generated method stub
					}
				});
		evolve(100,0,new ElapsedTime(60000));
	}

}
