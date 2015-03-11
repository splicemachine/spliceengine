package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;
/*
 * 
 * Split Scanner for multiple region scanners
 * 
 */
public class SplitRegionScanner extends BaseSplitRegionScanner<KeyValue> {
    protected static final Logger LOG = Logger.getLogger(SplitRegionScanner.class);
	
	public SplitRegionScanner(Scan scan, HTable table, List<HRegionLocation> locations) throws IOException {
		super(scan,table,locations);			
	}
	
	void createAndRegisterClientSideRegionScanner(HTable table, Scan newScan) throws IOException {
	  ClientSideRegionScanner clientSideRegionScanner = 
				  new ClientSideRegionScanner(table,table.getConfiguration(),FSUtils.getCurrentFileSystem(table.getConfiguration()), FSUtils.getRootDir(table.getConfiguration()),
					table.getTableDescriptor(),table.getRegionLocation(newScan.getStartRow()).getRegionInfo(),
					newScan,null);
				this.region = clientSideRegionScanner.region;
				registerRegionScanner(clientSideRegionScanner);			    			  

	}

	@Override
	List<HRegionLocation> getRegionsInRange(Scan scan) throws IOException {
		htable.clearRegionCache();
		return htable.getRegionsInRange(scan.getStartRow(), scan.getStopRow());
	}

	@Override
	public boolean isFilterDone() {
		return currentScanner.isFilterDone();
	}

	@Override
	public boolean nextRaw(List<KeyValue> result, String metric)
			throws IOException {
		return this.nextInternal(result);

	}

	@Override
	public boolean nextRaw(List<KeyValue> result, int limit, String metric)
			throws IOException {
		return this.nextInternal(result);

	}

	@Override
	public boolean next(List<KeyValue> results) throws IOException {
		return this.nextInternal(results);
	}

	@Override
	public boolean next(List<KeyValue> results, String metric)
			throws IOException {
		return this.nextInternal(results);
	}

	@Override
	public boolean next(List<KeyValue> result, int limit) throws IOException {
		return this.nextInternal(result);
	}

	@Override
	public boolean next(List<KeyValue> result, int limit, String metric)
			throws IOException {
		return this.nextInternal(result);
	}
	
}
