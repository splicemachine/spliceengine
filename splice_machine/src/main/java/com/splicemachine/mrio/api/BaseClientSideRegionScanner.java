package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.regionserver.BaseHRegionUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.mrio.MRConstants;

/**
 * 
 * 
 */
public abstract class BaseClientSideRegionScanner<T> implements RegionScanner {
    protected static final Logger LOG = Logger.getLogger(BaseClientSideRegionScanner.class);
	protected HRegion region;
	protected RegionScanner scanner;
	Configuration conf;
	FileSystem fs;
	Path rootDir;
	HTableDescriptor htd;
	HRegionInfo hri;
	Scan scan;
	T topCell;
	protected List<KeyValueScanner>	memScannerList = new ArrayList<KeyValueScanner>(1);
	protected boolean flushed;
	protected HTable table;
	SDataLib dataLib = HTransactorFactory.getTransactor().getDataLib();
	
	
	public BaseClientSideRegionScanner(HTable htable, Configuration conf, FileSystem fs,
			Path rootDir, HTableDescriptor htd, HRegionInfo hri, Scan scan,
			ScanMetrics scanMetrics)
			throws IOException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "init for regionInfo=%s, scan=%s", hri,scan);
		this.table = htable;
		scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED); 
		this.conf = conf;
		this.fs = fs;
		this.rootDir = rootDir;
		this.htd = htd;
		this.hri = hri;
		this.scan = scan;
		updateScanner();
	}
	
	public void close() throws IOException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "close");
		if (scanner != null)
			scanner.close();
		if (table != null)
			table.close();
		memScannerList.get(0).close();		
	}

	public HRegionInfo getRegionInfo() {
		return scanner.getRegionInfo();
	}

	public boolean reseek(byte[] row) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "reseek row=%s",row);
		return scanner.reseek(row);
	}
	
	public long getMvccReadPoint() {
		return scanner.getMvccReadPoint();
	}
	
	private boolean matchingFamily(List<T> result, byte[] family) {
		if (result.isEmpty())
			return false;
		int size = result.size();
		for (int i = 0; i<size; i++) {
			if (dataLib.singleMatchingFamily(result.get(i), family))
				return true;
		}
		return false;
	}
	
	public boolean nextInternalRaw(List<T> result) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "nextRaw");
		boolean res = dataLib.regionScannerNextRaw(scanner, result);
		if (matchingFamily(result,MRConstants.HOLD)) {
			result.clear();
			return nextInternalRaw(result);			
		}
		return updateTopCell(res,result);
	}

	private boolean updateTopCell(boolean response, List<T> results) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "updateTopCell from results%s",results);
		if (!results.isEmpty() &&
				(dataLib.singleMatchingFamily(results.get(0), MRConstants.FLUSH))
				) {
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "flush handling inititated");
			flushed = true;
			updateScanner();
			results.clear();
			return nextInternalRaw(results);
		} else 
			if (response)
				topCell = results.get(results.size() - 1);
			return response;
	}

	public abstract HRegion openHRegion() throws IOException;

	/**
	 * refresh underlying RegionScanner we call this when new store file gets
	 * created by MemStore flushes or current scanner fails due to compaction
	 */
	public void updateScanner() throws IOException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "updateScanner with hregionInfo=%s, tableName=%s, rootDir=%s, scan=%s",hri,htd.getNameAsString(), rootDir, scan);	
		if (!flushed)
			memScannerList.add(getMemStoreScanner());
		this.region = openHRegion();
		if (flushed) {
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "I am flushed");				
			if (scanner != null)
				scanner.close();
			if (this.topCell != null) {
				if (LOG.isTraceEnabled())
					SpliceLogUtils.trace(LOG, "setting start row to %s", topCell);		
				scan.setStartRow(dataLib.getDataRow(topCell)); 
			}
			scan.setAttribute(MRConstants.SPLICE_SCAN_MEMSTORE_ONLY, SIConstants.FALSE_BYTES);
			this.scanner = BaseHRegionUtil.getScanner(region, scan,null);
		}
		else {
			this.scanner = BaseHRegionUtil.getScanner(region, scan,memScannerList);			
		}
		
	}
	
	

	abstract KeyValueScanner getMemStoreScanner() throws IOException;
	
}