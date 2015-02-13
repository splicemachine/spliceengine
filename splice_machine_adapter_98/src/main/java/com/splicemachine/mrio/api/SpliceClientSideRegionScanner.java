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
<<<<<<< Updated upstream

=======
import org.apache.log4j.Logger;
>>>>>>> Stashed changes
/**
 * 
 * A Client Side Region Scanner that reads from store files and incremental
 * scans from memory store.
 * 
 * 
 */
public class SpliceClientSideRegionScanner implements RegionScanner {
<<<<<<< Updated upstream
	protected HRegion region;
	protected RegionScanner scanner;

	Configuration conf;
	FileSystem fs;
	Path rootDir;
	HTableDescriptor htd;
	HRegionInfo hri;
	Scan scan;
	Cell topCell;

	public SpliceClientSideRegionScanner(Configuration conf, FileSystem fs,
			Path rootDir, HTableDescriptor htd, HRegionInfo hri, Scan scan,
			ScanMetrics scanMetrics, List<KeyValueScanner> keyValueScanners)
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
	
	private void updateTopCell(List<Cell> results) {
		topCell = results.get(results.size() - 1);
	}
	
	@Override
	public boolean next(List<Cell> results) throws IOException {
		try{
			boolean result = scanner.next(results);
			updateTopCell(results);
			return result;
		} catch(IOException e){
			// We can catch IOException
			// when memory store flush occurs
			// when compaction completes and some files get removed (archived)
			updateScanner();
			results.clear();
			// TODO: recursive danger in case we 
			// have real issue
			return next(results);
		}
	}

	@Override
	public boolean next(List<Cell> results, int limit) throws IOException {
		try{
			boolean result = scanner.next(results, limit);
			updateTopCell(results);
			return result;
		} catch(IOException e){
			// We can catch IOException
			// when memory store flush occurs
			// when compaction completes and some files get removed (archived)
			updateScanner();
			results.clear();
			// TODO: recursive danger in case we 
			// have real issue
			return next(results, limit);
		}
	}

	@Override
	public void close() throws IOException {
		if(scanner != null) scanner.close();
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
	public long getMaxResultSize() {
		return scanner.getMaxResultSize();
	}

	@Override
	public long getMvccReadPoint() {
		return scanner.getMvccReadPoint();
	}

	@Override
	public boolean nextRaw(List<Cell> result) throws IOException {
		try{
			boolean res = scanner.nextRaw(result);
			updateTopCell(result);
			return res;
		} catch(IOException e){
			updateScanner();
			result.clear();
			// TODO: add code to avoid StackOverflowException
			//       and re-throw exception after X attempts
			return nextRaw(result);
		}
	}

	@Override
	public boolean nextRaw(List<Cell> result, int limit) throws IOException {
		try{
			boolean res = scanner.nextRaw(result, limit);
			updateTopCell(result);
			return res;
		} catch(IOException e){
			updateScanner();
			result.clear();
			// TODO: add code to avoid StackOverflowException
			//       and re-throw exception after X attempts
			return nextRaw(result, limit);
		}	}

	/**
	 * refresh underlying RegionScanner we call this when new store file gets
	 * created by MemStore flushes or current scanner fails due to compaction
	 */
	public void updateScanner() throws IOException {
		close();
		// open region from the root directory
		this.region = HRegion.openHRegion(conf, fs, rootDir, hri, htd, null,
				null, null);
		List<KeyValueScanner> memScannerList = getMemStoreScannerList();
		this.scanner = BaseHRegionUtil.getScanner(this.region, this.scan,
				memScannerList);
		if (topCell != null) {
			this.scanner.reseek(topCell.getRow());

		}
	}

	private List<KeyValueScanner> getMemStoreScannerList() throws IOException {
		List<KeyValueScanner> list = new ArrayList<KeyValueScanner>();
		HConnection connection = HConnectionManager.createConnection(conf);
		HTable table = (HTable) connection.getTable(htd.getName());
		Scan memScan = new Scan(scan);
		// Mark this scanner as memory only
		memScan.setAttribute( "memstore-only", "true".getBytes());
		if(topCell != null) {
			memScan.setStartRow(topCell.getRow());
		}
		// TODO : close table and connection. When?
		// FIXME!!! resource leak here.
		list.add(new SpliceMemstoreKeyValueScanner(table.getScanner(memScan)));
		return list;
	}
=======
    protected static final Logger LOG = Logger.getLogger(SpliceMemstoreKeyValueScanner.class);

  protected HRegion region;
  protected RegionScanner scanner;

  public SpliceClientSideRegionScanner(Configuration conf, FileSystem fs,
      Path rootDir, HTableDescriptor htd, HRegionInfo hri, Scan scan, ScanMetrics scanMetrics, List<KeyValueScanner> keyValueScanners) throws IOException {
    scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);     // region is immutable, set isolation level
    this.region = HRegion.openHRegion(conf, fs, rootDir, hri, htd, null, null, null);
    this.scanner = BaseHRegionUtil.getScanner(region, scan, keyValueScanners);
    region.startRegionOperation();
  }

@Override
public boolean next(List<Cell> results) throws IOException {
	return scanner.next(results);
}

@Override
public boolean next(List<Cell> result, int limit) throws IOException {
	return scanner.next(result,limit);
}

@Override
public void close() throws IOException {
	scanner.close();
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
public boolean nextRaw(List<Cell> result) throws IOException {
	return scanner.nextRaw(result);
}

@Override
public boolean nextRaw(List<Cell> result, int limit) throws IOException {
	return scanner.nextRaw(result, limit);
}
  
>>>>>>> Stashed changes
}