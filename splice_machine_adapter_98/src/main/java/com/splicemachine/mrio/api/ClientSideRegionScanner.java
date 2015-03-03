package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
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
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.mrio.MRConstants;

/**
 * 
 * 
 */
public class ClientSideRegionScanner implements RegionScanner {
    protected static final Logger LOG = Logger.getLogger(ClientSideRegionScanner.class);
	protected HRegion region;
	protected RegionScanner scanner;
	Configuration conf;
	FileSystem fs;
	Path rootDir;
	HTableDescriptor htd;
	HRegionInfo hri;
	Scan scan;
	Cell topCell;
	protected List<KeyValueScanner>	memScannerList = new ArrayList<KeyValueScanner>(1);
	protected boolean flushed;
	protected HTable table;
	
	
	public ClientSideRegionScanner(Configuration conf, FileSystem fs,
			Path rootDir, HTableDescriptor htd, HRegionInfo hri, Scan scan,
			ScanMetrics scanMetrics)
			throws IOException {
		scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED); 
		this.conf = conf;
		this.fs = fs;
		this.rootDir = rootDir;
		this.htd = htd;
		this.hri = hri;
		this.scan = scan;
		updateScanner();
	}
	
	
	@Override
	public boolean next(List<Cell> results) throws IOException {
		return nextRaw(results);
	}

	@Override
	public boolean next(List<Cell> results, int limit) throws IOException {
		return nextRaw(results);
	}

	@Override
	public void close() throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "close");
		if (scanner != null)
			scanner.close();
		if (table != null)
			table.close();
		memScannerList.get(0).close();		
	}

	@Override
	public HRegionInfo getRegionInfo() {
		return scanner.getRegionInfo();
	}

	@Override
	public boolean isFilterDone() throws IOException {
		return scanner.isFilterDone();
	}

	@Override
	public boolean reseek(byte[] row) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "reseek row=%s",row);
		return scanner.reseek(row);
	}

	@Override
	public long getMaxResultSize() {
		return scanner.getMaxResultSize();
	}

	@Override
	public long getMvccReadPoint() {
		return scanner.getMvccReadPoint();
	}

	@Override
	public boolean nextRaw(List<Cell> result, int limit) throws IOException {
		return nextRaw(result);
	}
	
	private boolean matchingFamily(List<Cell> result, byte[] family) {
		if (result.isEmpty())
			return false;
		int size = result.size();
		for (int i = 0; i<size; i++) {
			if (CellUtils.singleMatchingFamily(result.get(i), family))
				return true;
		}
		return false;
	}
	
	@Override
	public boolean nextRaw(List<Cell> result) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "nextRaw");
		boolean res = scanner.nextRaw(result);
		if (matchingFamily(result,MRConstants.HOLD)) {
			result.clear();
			return nextRaw(result);			
		}
		return updateTopCell(res,result);
	}

	private boolean updateTopCell(boolean response, List<Cell> results) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "updateTopCell from results%s",results);
		if (!results.isEmpty() &&
				(CellUtils.singleMatchingFamily(results.get(0), MRConstants.FLUSH))
				) {
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "flush handling inititated");
			flushed = true;
			updateScanner();
			results.clear();
			return nextRaw(results);
		} else 
			if (response)
				topCell = results.get(results.size() - 1);
			return response;
	}

	

	/**
	 * refresh underlying RegionScanner we call this when new store file gets
	 * created by MemStore flushes or current scanner fails due to compaction
	 */
	public void updateScanner() throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "updateScanner with hregionInfo=%s, tableName=%s, rootDir=%s, scan=%s",hri,htd.getNameAsString(), rootDir, scan);	
		if (!flushed)
			memScannerList.add(getMemStoreScanner());
		this.region = HRegion.openHRegion(conf, fs, rootDir, hri, htd, null,null, null);
		if (flushed) {
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "I am flushed");				
			if (scanner != null)
				scanner.close();
			if (this.topCell != null) {
				if (LOG.isTraceEnabled())
					SpliceLogUtils.trace(LOG, "setting start row to %s", topCell);				
				scan.setStartRow(topCell.getRow()); // Need to fix... JL
			}
			scan.setAttribute(MRConstants.SPLICE_SCAN_MEMSTORE_ONLY, SIConstants.FALSE_BYTES);
			this.scanner = BaseHRegionUtil.getScanner(region, scan,null);
		}
		else {
			this.scanner = BaseHRegionUtil.getScanner(region, scan,memScannerList);			
		}
		
	}

	private KeyValueScanner getMemStoreScanner() throws IOException {
		HConnection connection = HConnectionManager.createConnection(conf);
		table = (HTable) connection.getTable(htd.getName());
		Scan memScan = new Scan(scan);
		memScan.setAttribute( MRConstants.SPLICE_SCAN_MEMSTORE_ONLY,SIConstants.TRUE_BYTES);
		return new MemstoreKeyValueScanner(table.getScanner(memScan));
	}
}