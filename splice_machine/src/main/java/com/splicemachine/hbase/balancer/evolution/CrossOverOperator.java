package com.splicemachine.hbase.balancer.evolution;

import java.util.List;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.uncommons.maths.random.Probability;
import org.uncommons.watchmaker.framework.operators.ListCrossover;

/**
 * 
 * The Evolutionary Operator reflecting the mating of two lists based on their number of crossover points and their crossover probability.
 * 
 *
 */
public class CrossOverOperator extends ListCrossover<List<RegionPlan>> {

	protected CrossOverOperator(int crossoverPoints,Probability crossoverProbability) {
		super(crossoverPoints, crossoverProbability);
	}


}
