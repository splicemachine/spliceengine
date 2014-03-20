package com.splicemachine.hbase.balancer.evolution;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.Bytes;
import org.uncommons.watchmaker.framework.EvolutionEngine;
import org.uncommons.watchmaker.framework.EvolutionObserver;
import org.uncommons.watchmaker.framework.PopulationData;
import org.uncommons.watchmaker.framework.termination.ElapsedTime;
import com.splicemachine.hbase.balancer.BaseLoadBalancer;
import com.splicemachine.hbase.balancer.RegionLocationFinder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SpliceLoadBalancer extends BaseLoadBalancer {
    private static final Log LOG = LogFactory.getLog(SpliceLoadBalancer.class);
    private final RegionLocationFinder regionFinder = new RegionLocationFinder();
    private ClusterStatus clusterStatus = null;
    private Map<String, List<RegionLoad>> loads = new HashMap<String, List<RegionLoad>>();
    private static final String KEEP_REGION_LOADS = "hbase.master.balancer.stochastic.numRegionLoadsToRemember";    
    private int numRegionLoadsToRemember = 15;
    private SpliceLayoutFitness layoutFitness = new SpliceLayoutFitness();

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        regionFinder.setConf(conf);
        numRegionLoadsToRemember = conf.getInt(KEEP_REGION_LOADS, numRegionLoadsToRemember);
        layoutFitness.setConf(conf);
    }

    @Override
    public void setClusterStatus(ClusterStatus st) {
        super.setClusterStatus(st);
        regionFinder.setClusterStatus(st);
        this.clusterStatus = st;
        updateRegionLoad();
    }

    @Override
    public void setMasterServices(MasterServices masterServices) {
        super.setMasterServices(masterServices);
        this.services = masterServices;
        this.regionFinder.setServices(masterServices);
    }
    
    /**
     * Given the cluster state this will try and approach an optimal balance. This
     * should always approach the optimal state given enough steps.
     */
    @Override
    public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState) {
        if (clusterState.size() <= 1) {
            LOG.debug("Skipping load balance as cluster has only one node.");
            return null;
        }
        EvolutionEngine<List<RegionPlan>> darwin = new SpliceLayoutGenerationalEvolutionEngine(clusterState);
        darwin.addEvolutionObserver(new EvolutionObserver<List<RegionPlan>>() {
			@Override
			public void populationUpdate(PopulationData<? extends List<RegionPlan>> data) {
				LOG.debug("Updating Population " + data.getElapsedTime());
			}
		});
        return darwin.evolve(100,0,new ElapsedTime(60000));        
    }

    /** Store the current region loads. */
    private synchronized void updateRegionLoad() {

        //We create a new hashmap so that regions that are no longer there are removed.
        //However we temporarily need the old loads so we can use them to keep the rolling average.
        Map<String, List<RegionLoad>> oldLoads = loads;
        loads = new HashMap<String, List<RegionLoad>>();

        for (ServerName sn : clusterStatus.getServers()) {
            ServerLoad sl = clusterStatus.getLoad(sn);
            if (sl == null) continue;
            for (Entry<byte[], RegionLoad> entry : sl.getRegionsLoad().entrySet()) {
                List<RegionLoad> rLoads = oldLoads.get(Bytes.toString(entry.getKey()));
                if (rLoads != null) {

                    //We're only going to keep 15.  So if there are that many already take the last 14
                    if (rLoads.size() >= numRegionLoadsToRemember) {
                        int numToRemove = 1 +  (rLoads.size() - numRegionLoadsToRemember);

                        rLoads = rLoads.subList(numToRemove, rLoads.size());
                    }

                } else {
                    //There was nothing there
                    rLoads = new ArrayList<RegionLoad>();
                }
                rLoads.add(entry.getValue());
                loads.put(Bytes.toString(entry.getKey()), rLoads);
            }
        }
    }

	@Override
	public void initialize() throws HBaseIOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop(String why) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isStopped() {
		// TODO Auto-generated method stub
		return false;
	}
}

