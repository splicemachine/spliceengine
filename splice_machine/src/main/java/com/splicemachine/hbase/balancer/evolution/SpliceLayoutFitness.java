package com.splicemachine.hbase.balancer.evolution;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.uncommons.watchmaker.framework.FitnessEvaluator;

public class SpliceLayoutFitness implements FitnessEvaluator<List<RegionPlan>> {
    private static final String STOREFILE_SIZE_COST_KEY =
            "hbase.master.balancer.stochastic.storefileSizeCost";
    private static final String MEMSTORE_SIZE_COST_KEY =
            "hbase.master.balancer.stochastic.memstoreSizeCost";
    private static final String WRITE_REQUEST_COST_KEY =
            "hbase.master.balancer.stochastic.writeRequestCost";
    private static final String READ_REQUEST_COST_KEY =
            "hbase.master.balancer.stochastic.readRequestCost";
    private static final String LOCALITY_COST_KEY = "hbase.master.balancer.stochastic.localityCost";
    private static final String TABLE_LOAD_COST_KEY =
            "hbase.master.balancer.stochastic.tableLoadCost";
    private static final String MOVE_COST_KEY = "hbase.master.balancer.stochastic.moveCost";
    private static final String REGION_LOAD_COST_KEY =
            "hbase.master.balancer.stochastic.regionLoadCost";
    private static final String STEPS_PER_REGION_KEY =
            "hbase.master.balancer.stochastic.stepsPerRegion";
    private static final String MAX_STEPS_KEY = "hbase.master.balancer.stochastic.maxSteps";
    private static final String MAX_MOVES_KEY = "hbase.master.balancer.stochastic.maxMoveRegions";
    private Map<String, List<HServerLoad.RegionLoad>> loads = new HashMap<String, List<HServerLoad.RegionLoad>>();

    // values are defaults
    private int maxSteps = 15000;
    private int stepsPerRegion = 110;
    private int maxMoves = 600;
    private float loadMultiplier = 55;
    private float moveCostMultiplier = 5;
    private float tableMultiplier = 5;
    private float localityMultiplier = 5;
    private float readRequestMultiplier = 0;
    private float writeRequestMultiplier = 0;
    private float memStoreSizeMultiplier = 5;
    private float storeFileSizeMultiplier = 5;

    public SpliceLayoutFitness() {
    	
    }
    
    
    public void setConf(Configuration conf) {
        maxSteps = conf.getInt(MAX_STEPS_KEY, maxSteps);
        maxMoves = conf.getInt(MAX_MOVES_KEY, maxMoves);
        stepsPerRegion = conf.getInt(STEPS_PER_REGION_KEY, stepsPerRegion);

        // Load multiplier should be the greatest as it is the most general way to balance data.
        loadMultiplier = conf.getFloat(REGION_LOAD_COST_KEY, loadMultiplier);

        // Move cost multiplier should be the same cost or higer than the rest of the costs to ensure
        // that two costs must get better to justify a move cost.
        moveCostMultiplier = conf.getFloat(MOVE_COST_KEY, moveCostMultiplier);

        // These are the added costs so that the stochastic load balancer can get a little bit smarter
        // about where to move regions.
        tableMultiplier = conf.getFloat(TABLE_LOAD_COST_KEY, tableMultiplier);
        localityMultiplier = conf.getFloat(LOCALITY_COST_KEY, localityMultiplier);
        memStoreSizeMultiplier = conf.getFloat(MEMSTORE_SIZE_COST_KEY, memStoreSizeMultiplier);
        storeFileSizeMultiplier = conf.getFloat(STOREFILE_SIZE_COST_KEY, storeFileSizeMultiplier);
        readRequestMultiplier = conf.getFloat(READ_REQUEST_COST_KEY, readRequestMultiplier);
        writeRequestMultiplier = conf.getFloat(WRITE_REQUEST_COST_KEY, writeRequestMultiplier);
    }

    
    /**
     * Lower the cost, the better so isNatural returns false.
     * 
     */
	@Override
	public boolean isNatural() {
		return false;
	}

	@Override
	public double getFitness(List<RegionPlan> candidate,List<? extends List<RegionPlan>> population) {
		/*
		int numberOfMoves = 0;
		for (RegionPlan regionPlan: candidate) {
			regionPlan.getRegionInfo().get
			if (regionPlan.getDestination().equals(regionPlan.getSource()))
				numberOfMoves++;				
		}
		
		double moveCost = moveCostMultiplier * computeMoveCost()
		
		
		
		
        double moveCost = moveCostMultiplier * computeMoveCost(initialRegionMapping, clusterState);

        double regionCountSkewCost = loadMultiplier * computeSkewLoadCost(clusterState);
        double tableSkewCost = tableMultiplier * computeTableSkewLoadCost(clusterState);
        double localityCost =
                localityMultiplier * computeDataLocalityCost(initialRegionMapping, clusterState);

        double memstoreSizeCost =
                memStoreSizeMultiplier
                        * computeRegionLoadCost(clusterState, RegionLoadCostType.MEMSTORE_SIZE);
        double storefileSizeCost =
                storeFileSizeMultiplier
                        * computeRegionLoadCost(clusterState, RegionLoadCostType.STOREFILE_SIZE);


        double readRequestCost =
                readRequestMultiplier
                        * computeRegionLoadCost(clusterState, RegionLoadCostType.READ_REQUEST);
        double writeRequestCost =
                writeRequestMultiplier
                        * computeRegionLoadCost(clusterState, RegionLoadCostType.WRITE_REQUEST);

        double total =
                moveCost + regionCountSkewCost + tableSkewCost + localityCost + memstoreSizeCost
                        + storefileSizeCost + readRequestCost + writeRequestCost;
        LOG.trace("Computed weights for a potential balancing total = " + total + " moveCost = "
                + moveCost + " regionCountSkewCost = " + regionCountSkewCost + " tableSkewCost = "
                + tableSkewCost + " localityCost = " + localityCost + " memstoreSizeCost = "
                + memstoreSizeCost + " storefileSizeCost = " + storefileSizeCost);
        return total;
        */
		return 0;
		
		
		
		
		
		
	}
	
	public static SpliceLayoutFitness create() {
		return new SpliceLayoutFitness();
	}
	
	   /**
     * Given the starting state of the regions and a potential ending state
     * compute cost based upon the number of regions that have moved.
     *
     * @param initialRegionMapping The starting location of regions.
     * @param clusterState         The potential new cluster state.
     * @return The cost. Between 0 and 1.
     */
    double computeMoveCost(Map<HRegionInfo, ServerName> initialRegionMapping,
                           Map<ServerName, List<HRegionInfo>> clusterState) {
        float moveCost = 0;
        for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
            for (HRegionInfo region : entry.getValue()) {
                if (initialRegionMapping.get(region) != entry.getKey()) {
                    moveCost += 1;
                }
            }
        }

        //Don't let this single balance move more than the max moves.
        //This allows better scaling to accurately represent the actual cost of a move.
        if (moveCost > maxMoves) {
            return 10000;   //return a number much greater than any of the other cost functions
        }

        return scale(0, Math.min(maxMoves, initialRegionMapping.size()), moveCost);
    }

    /**
     * Compute the cost of a potential cluster state from skew in number of
     * regions on a cluster
     *
     * @param clusterState The proposed cluster state
     * @return The cost of region load imbalance.
     */
    double computeSkewLoadCost(Map<ServerName, List<HRegionInfo>> clusterState) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        for (List<HRegionInfo> regions : clusterState.values()) {
            int size = regions.size();
            stats.addValue(size);
        }
        return costFromStats(stats);
    }

    /**
     * Compute the cost of a potential cluster configuration based upon how evenly
     * distributed tables are.
     *
     * @param clusterState Proposed cluster state.
     * @return Cost of imbalance in table.
     */
    double computeTableSkewLoadCost(Map<ServerName, List<HRegionInfo>> clusterState) {

        Map<String, MutableInt> tableRegionsTotal = new HashMap<String, MutableInt>();
        Map<String, MutableInt> tableRegionsOnCurrentServer = new HashMap<String, MutableInt>();
        Map<String, Integer> tableCostSeenSoFar = new HashMap<String, Integer>();
        // Go through everything per server
        for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
            tableRegionsOnCurrentServer.clear();

            // For all of the regions count how many are from each table
            for (HRegionInfo region : entry.getValue()) {
                String tableName = region.getTableNameAsString();

                // See if this table already has a count on this server
                MutableInt regionsOnServerCount = tableRegionsOnCurrentServer.get(tableName);

                // If this is the first time we've seen this table on this server
                // create a new mutable int.
                if (regionsOnServerCount == null) {
                    regionsOnServerCount = new MutableInt(0);
                    tableRegionsOnCurrentServer.put(tableName, regionsOnServerCount);
                }

                // Increment the count of how many regions from this table are host on
                // this server
                regionsOnServerCount.increment();

                // Now count the number of regions in this table.
                MutableInt totalCount = tableRegionsTotal.get(tableName);

                // If this is the first region from this table create a new counter for
                // this table.
                if (totalCount == null) {
                    totalCount = new MutableInt(0);
                    tableRegionsTotal.put(tableName, totalCount);
                }
                totalCount.increment();
            }

            // Now go through all of the tables we have seen and keep the max number
            // of regions of this table a single region server is hosting.
            for (Entry<String, MutableInt> currentServerEntry: tableRegionsOnCurrentServer.entrySet()) {
                String tableName = currentServerEntry.getKey();
                Integer thisCount = currentServerEntry.getValue().toInteger();
                Integer maxCountSoFar = tableCostSeenSoFar.get(tableName);

                if (maxCountSoFar == null || thisCount.compareTo(maxCountSoFar) > 0) {
                    tableCostSeenSoFar.put(tableName, thisCount);
                }
            }
        }

        double max = 0;
        double min = 0;
        double value = 0;

        // Compute the min, value, and max.
        for (Entry<String, MutableInt> currentEntry : tableRegionsTotal.entrySet()) {
            max += tableRegionsTotal.get(currentEntry.getKey()).doubleValue();
            min += tableRegionsTotal.get(currentEntry.getKey()).doubleValue() / clusterState.size();
            value += tableCostSeenSoFar.get(currentEntry.getKey()).doubleValue();
        }
        return scale(min, max, value);
    }

    /**
     * Compute a cost of a potential cluster configuration based upon where
     * {@link org.apache.hadoop.hbase.regionserver.StoreFile}s are located.
     *
     * @param initialRegionMapping - not used
     * @param clusterState The state of the cluster
     * @return A cost between 0 and 1. 0 Means all regions are on the sever with
     *         the most local store files.
     */
    double computeDataLocalityCost(Map<HRegionInfo, ServerName> initialRegionMapping,
                                   Map<ServerName, List<HRegionInfo>> clusterState) {
/*
        double max = 0;
        double cost = 0;

        // If there's no master so there's no way anything else works.
        if (this.services == null) return cost;

        for (Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
            ServerName sn = entry.getKey();
            for (HRegionInfo region : entry.getValue()) {

                max += 1;

                List<ServerName> dataOnServers = regionFinder.getTopBlockLocations(region);

                // If we can't find where the data is getTopBlock returns null.
                // so count that as being the best possible.
                if (dataOnServers == null) {
                    continue;
                }

                int index = dataOnServers.indexOf(sn);
                if (index < 0) {
                    cost += 1;
                } else {
                    cost += (double) index / (double) dataOnServers.size();
                }

            }
        }
        return scale(0, max, cost);
        */ 
    	return 0.0;
    }

    /** The cost's that can be derived from RegionLoad */
    public enum RegionLoadCostType {
        READ_REQUEST, WRITE_REQUEST, MEMSTORE_SIZE, STOREFILE_SIZE
    }

    /**
     * Compute the cost of the current cluster state due to some RegionLoadCost type
     *
     * @param clusterState the cluster
     * @param costType     what type of cost to consider
     * @return the scaled cost.
     */
    private double computeRegionLoadCost(Map<ServerName, List<HRegionInfo>> clusterState,
                                         RegionLoadCostType costType) {
    	/*
        if (this.clusterStatus == null || this.loads == null || this.loads.size() == 0) return 0;

        DescriptiveStatistics stats = new DescriptiveStatistics();

        // For every server look at the cost of each region
        for (List<HRegionInfo> regions : clusterState.values()) {
            long cost = 0; //Cost this server has from RegionLoad

            // For each region
            for (HRegionInfo region : regions) {
                // Try and get the region using the regionNameAsString
                List<HServerLoad.RegionLoad> rl = loads.get(region.getRegionNameAsString());

                // That could have failed if the RegionLoad is using the other regionName
                if (rl == null) {
                    // Try getting the region load using encoded name.
                    rl = loads.get(region.getEncodedName());
                }
                // Now if we found a region load get the type of cost that was requested.
                if (rl != null) {
                    cost += getRegionLoadCost(rl, costType);
                }
            }

            // Add the total cost to the stats.
            stats.addValue(cost);
        }

        // No return the scaled cost from data held in the stats object.
        return costFromStats(stats);
        */
    	return 0.0;
    }

    /**
     * Get the un-scaled cost from a RegionLoad
     *
     * @param regionLoadList   the Region load List
     * @param type The type of cost to extract
     * @return the double representing the cost
     */
    private double getRegionLoadCost(List<HServerLoad.RegionLoad> regionLoadList, RegionLoadCostType type) {
        double cost = 0;

        int size = regionLoadList.size();
        for(int i =0; i< size; i++) {
            HServerLoad.RegionLoad rl = regionLoadList.get(i);
            double toAdd = 0;
            switch (type) {
                case READ_REQUEST:
                    toAdd =  rl.getReadRequestsCount();
                    break;
                case WRITE_REQUEST:
                    toAdd =  rl.getWriteRequestsCount();
                    break;
                case MEMSTORE_SIZE:
                    toAdd =  rl.getMemStoreSizeMB();
                    break;
                case STOREFILE_SIZE:
                    toAdd =  rl.getStorefileSizeMB();
                    break;
                default:
                    assert false : "RegionLoad cost type not supported.";
                    return 0;
            }

            if (cost == 0) {
                cost = toAdd;
            } else {
                cost = (.5 * cost) + (.5 * toAdd);
            }
        }

        return cost;

    }

    /**
     * Function to compute a scaled cost using {@link DescriptiveStatistics}. It
     * assumes that this is a zero sum set of costs.  It assumes that the worst case
     * possible is all of the elements in one region server and the rest having 0.
     *
     * @param stats the costs
     * @return a scaled set of costs.
     */
    double costFromStats(DescriptiveStatistics stats) {
        double totalCost = 0;
        double mean = stats.getMean();

        //Compute max as if all region servers had 0 and one had the sum of all costs.  This must be
        // a zero sum cost for this to make sense.
        double max = ((stats.getN() - 1) * stats.getMean()) + (stats.getSum() - stats.getMean());
        for (double n : stats.getValues()) {
            totalCost += Math.abs(mean - n);

        }

        return scale(0, max, totalCost);
    }

    /**
     * Scale the value between 0 and 1.
     *
     * @param min   Min value
     * @param max   The Max value
     * @param value The value to be scaled.
     * @return The scaled value.
     */
    private double scale(double min, double max, double value) {
        if (max == 0 || value == 0) {
            return 0;
        }

        return Math.max(0d, Math.min(1d, (value - min) / max));
    }
}
