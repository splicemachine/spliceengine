package com.splicemachine.mrio.api.core;

import java.io.IOException;
import java.util.List;
import com.splicemachine.access.hbase.HBaseTableFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;
/*
 * 
 * Split Scanner for multiple region scanners
 * 
 */
public class SplitRegionScanner extends BaseSplitRegionScanner<Cell> {
    protected static final Logger LOG = Logger.getLogger(SplitRegionScanner.class);
	
	public SplitRegionScanner(Scan scan, Table table, List<HRegionLocation> locations) throws IOException {
		super(scan,table,locations);

	}

	@Override
	public boolean nextRaw(List<Cell> result) throws IOException {
		return this.nextInternal(result);
	}

	@Override
	public boolean nextRaw(List<Cell> result, int limit) throws IOException {
		return this.nextInternal(result);
	}

	@Override
	public boolean next(List<Cell> results) throws IOException {
		return this.nextInternal(results);
	}

	@Override
	public boolean next(List<Cell> result, int limit) throws IOException {
		return this.nextInternal(result);
	}
	
	void createAndRegisterClientSideRegionScanner(Table table, Scan newScan) throws IOException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "createAndRegisterClientSideRegionScanner with table=%s, scan=%s, tableConfiguration=%s",table,newScan, table.getConfiguration());
        Configuration conf = table.getConfiguration();
        if (System.getProperty("hbase.rootdir") != null)
            conf.set("hbase.rootdir",System.getProperty("hbase.rootdir"));

        try {
            ClientSideRegionScanner clientSideRegionScanner =
                    new ClientSideRegionScanner(table, conf, FSUtils.getCurrentFileSystem(conf), FSUtils.getRootDir(conf),
                            table.getTableDescriptor(), HBaseTableFactory.getInstance().getRegionInRange(table.getName().getQualifier(),newScan.getStartRow()).getRegionInfo(),
                            newScan, null);
            this.region = clientSideRegionScanner.region;
            registerRegionScanner(clientSideRegionScanner);
        } catch (Exception e) {
            throw new IOException(e);
        }
	}

	@Override
	List<HRegionLocation> getRegionsInRange(Scan scan) throws IOException {
        try {
            return HBaseTableFactory.getInstance().getRegionsInRange(htable.getName().getQualifier(),scan.getStartRow(), scan.getStopRow());
        } catch (Exception e) {
            throw new IOException(e);
        }
	}

	@Override
	public boolean isFilterDone() throws IOException {
		return currentScanner.isFilterDone();
	}

	@Override
	public long getMaxResultSize() {
		return currentScanner.getMaxResultSize();
	}
		
	
	
}
