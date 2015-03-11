package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.mrio.MRConstants;

/**
 * 
 * 
 */
public class ClientSideRegionScanner extends BaseClientSideRegionScanner<Cell> {	
	public ClientSideRegionScanner(HTable table, Configuration conf, FileSystem fs,
			Path rootDir, HTableDescriptor htd, HRegionInfo hri, Scan scan,
			ScanMetrics scanMetrics)
			throws IOException {
		super(table, conf,fs,rootDir,htd,hri,scan,scanMetrics);
	}
		
	@Override
	public boolean next(List<Cell> results) throws IOException {
		return nextInternalRaw(results);
	}

	@Override
	public boolean next(List<Cell> results, int limit) throws IOException {
		return nextInternalRaw(results);
	}
	@Override
	public long getMaxResultSize() {
		return scanner.getMaxResultSize();
	}
	
	@Override
	public boolean isFilterDone() throws IOException {
		return scanner.isFilterDone();
	}

	@Override
	public boolean nextRaw(List<Cell> result, int limit) throws IOException {
		return nextInternalRaw(result);
	}

	@Override
	public boolean nextRaw(List<Cell> result) throws IOException {
		return nextInternalRaw(result);
	}

	@Override
	public HRegion openHRegion() throws IOException {
		return HRegion.openHRegion(conf, fs, rootDir, hri, htd, null,null, null);
	}
	
	KeyValueScanner getMemStoreScanner() throws IOException {
		Scan memScan = new Scan(scan);
		memScan.setAttribute( MRConstants.SPLICE_SCAN_MEMSTORE_ONLY,SIConstants.TRUE_BYTES);
		return new MemstoreKeyValueScanner(table.getScanner(memScan));
	}
	
}