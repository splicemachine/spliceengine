package com.splicemachine.pipeline;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

import com.splicemachine.pipeline.api.Service;
import com.splicemachine.pipeline.coprocessor.BatchProtocol;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;
/**
 * Placeholder for pieces of the Splice Driver that we can migrate to a stand alone
 * pipeline driver.
 * 
 *
 */
public class PipelineDriver {
/*    protected RegionServerServices regionServerServices;
	public static WriteCoordinator writeCoordinator;
    private static MetricsRegistry spliceMetricsRegistry = null;
    private static JmxReporter metricsReporter;
    private static PipelineDriver INSTANCE;
	public static String ENDPOINT_CLASS_NAME;
    private final List<Service> services = new CopyOnWriteArrayList<Service>();


	private PipelineDriver(Configuration configuration, String endpointName) throws IOException {
		writeCoordinator = WriteCoordinator.create(configuration);
		spliceMetricsRegistry = new MetricsRegistry();
        metricsReporter = new JmxReporter(spliceMetricsRegistry);
        metricsReporter.start();
        ENDPOINT_CLASS_NAME = endpointName;
	}
	
	public static void initialize(Configuration configuration, String endpointName) throws IOException {
		INSTANCE = new PipelineDriver(configuration,endpointName);
	}
	
	public WriteCoordinator getWriteCoordinator() {
		return writeCoordinator;
	}
	public void shutdown() {
		metricsReporter.shutdown();	
		spliceMetricsRegistry.shutdown();
	}
	
	public static PipelineDriver getPipelineDriver() {
		return INSTANCE;
	}
    public MetricsRegistry getRegistry(){
        return spliceMetricsRegistry;
    }
    
    public void start (RegionServerServices regionServerServices) {
    	this.regionServerServices = regionServerServices;
    }

    public ServerName getServerName() {
    	return regionServerServices.getServerName();
    }
    
    public List<HRegion> getOnlineRegionsForTable(byte[] tableName) throws IOException {
    	return regionServerServices.getOnlineRegions(tableName);
    }

    public HRegion getOnlineRegion(String encodedRegionName)  {
    	return regionServerServices.getFromOnlineRegions(encodedRegionName);
    }

    public RegionCoprocessorHost getCoprocessorHost(String encodedRegionName) {
    	HRegion region = getOnlineRegion(encodedRegionName);
    	return region==null?null:region.getCoprocessorHost();    	
    }
    
    public CoprocessorEnvironment getSpliceIndexEndpointEnvironment(String encodedRegionName) {
    	RegionCoprocessorHost host = getCoprocessorHost(encodedRegionName);
    	return host==null?null:host.findCoprocessorEnvironment(ENDPOINT_CLASS_NAME);
    }

    public BatchProtocol getBatchEndpoint(String encodedRegionName) {
    	CoprocessorEnvironment ce = getSpliceIndexEndpointEnvironment(encodedRegionName);
    	return ce == null?null:(BatchProtocol) ce.getInstance();
    }
    
    public void registerService(Service service){
        this.services.add(service);
        //If the service is registered after we've successfully started up, let it know on the same thread.
//        if(stateHolder.get()==State.RUNNING)
            service.start();// -- JL?
    }

    public void deregisterService(Service service){
        this.services.remove(service);
    }
    */
}
