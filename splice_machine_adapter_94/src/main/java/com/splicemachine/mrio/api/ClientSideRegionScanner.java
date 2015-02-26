package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.utils.SpliceLogUtils;

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
	KeyValue topCell;
	protected List<KeyValueScanner> memScannerList = new ArrayList<KeyValueScanner>(1);
	protected boolean flushed;
	protected HTable htable;
	
	
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
	public boolean next(List<KeyValue> results) throws IOException {
		return nextRaw(results);
	}

	@Override
	public boolean next(List<KeyValue> results, int limit) throws IOException {
		return nextRaw(results);
	}

	@Override
	public void close() throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "close");
		if (scanner != null)
				scanner.close();
		if (htable != null)
				htable.close();
		memScannerList.get(0).close();		
	}

	@Override
	public HRegionInfo getRegionInfo() {
		return scanner.getRegionInfo();
	}

	@Override
	public boolean isFilterDone() {
		return scanner.isFilterDone();
	}

	@Override
	public boolean reseek(byte[] row) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "reseek row=%s",row);
		try{
			return scanner.reseek(row);
		} catch(IOException e){
			updateScanner();
			// TODO: add code to avoid StackOverflowException
			//       and re-throw exception after X attempts
			return reseek(row);
		}
	}

	@Override
	public long getMvccReadPoint() {
		return scanner.getMvccReadPoint();
	}

	public boolean nextRaw(List<KeyValue> result, int limit) throws IOException {
		return nextRaw(result);
	}
	
	public boolean nextRaw(List<KeyValue> result) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "nextRaw");
		boolean res = scanner.nextRaw(result, null);
		if (res)
			return updateTopCell(result);
		else
			return false;
	}

	private boolean updateTopCell(List<KeyValue> results) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "updateTopCell from results%s",results);
		topCell = results.get(results.size() - 1);
		if (Bytes.equals(topCell.getFamily(),MRConstants.FLUSH)) {
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "updateTopCell: Flush Occurred");
			flushed = true;
			updateScanner();
			results.clear();
			return nextRaw(results);
		} else 
			return true;
	}

	

	/**
	 * refresh underlying RegionScanner we call this when new store file gets
	 * created by MemStore flushes or current scanner fails due to compaction
	 */
	public void updateScanner() throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "updateScanner with hregionInfo=%s, tableName=%s, rootDir=%s, scan=%s",hri,htd.getNameAsString(), rootDir, scan);	
		if (flushed)
			memScannerList = null; // No Memstore scans needed since we flushed, breaks READ_UNCOMMITED
		else	
			memScannerList.add(getMemStoreScanner());
		this.region = HRegion.openHRegion(rootDir, hri, htd, null, conf);
		if (flushed) {
			if (scanner != null)
				scanner.close();
			if (this.topCell != null)
				scan.setStartRow(topCell.getRow()); // Need to fix... JL
			this.scanner = BaseHRegionUtil.getScanner(this.region, this.scan,null);
		}
		else {
			this.scanner = BaseHRegionUtil.getScanner(this.region, this.scan,memScannerList);			
		}
		
	}

	private KeyValueScanner getMemStoreScanner() throws IOException {
		HConnection connection = HConnectionManager.createConnection(conf);
		htable = new HTable(htd.getName());
		Scan memScan = new Scan(scan);
		memScan.setAttribute( MRConstants.SPLICE_SCAN_MEMSTORE_ONLY,SIConstants.EMPTY_BYTE_ARRAY);
		return new MemstoreKeyValueScanner(htable.getScanner(memScan));
	}


	@Override
	public boolean next(List<KeyValue> results, String arg1) throws IOException {
		// TODO Auto-generated method stub
		return nextRaw(results);
	}


	@Override
	public boolean next(List<KeyValue> results, int arg1, String arg2)
			throws IOException {
		return nextRaw(results);
	}


	@Override
	public boolean nextRaw(List<KeyValue> results, String arg1) throws IOException {
		return nextRaw(results);
	}


	@Override
	public boolean nextRaw(List<KeyValue> results, int arg1, String arg2)
			throws IOException {
		return nextRaw(results);
	}
}