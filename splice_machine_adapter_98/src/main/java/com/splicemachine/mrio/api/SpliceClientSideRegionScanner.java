package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.regionserver.BaseHRegionUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
/**
 * 
 * A Client Side Region Scanner that reads from store files and incremental scans from memstore.
 * 
 *
 */
public class SpliceClientSideRegionScanner implements RegionScanner {
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
  
}