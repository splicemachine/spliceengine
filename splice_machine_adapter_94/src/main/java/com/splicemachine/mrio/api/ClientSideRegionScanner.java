package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.mortbay.log.Log;

/**
 * A client scanner for a region opened for read-only on the client side. Assumes region data
 * is not changing.
 */
public class ClientSideRegionScanner extends AbstractClientScanner {

  private HRegion region;
  private Scan scan;
  RegionScanner scanner;
  List<KeyValue> values;

  public ClientSideRegionScanner(Configuration conf, FileSystem fs,
      Path rootDir, HTableDescriptor htd, HRegionInfo hri, Scan scan, ScanMetrics scanMetrics) throws IOException {
    this.scan = scan;
    // region is immutable, set isolation level
    this.scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
    // open region from the snapshot directory
    this.region = HRegion.openHRegion(rootDir, hri,htd, null, conf, null, null);
    // create an internal region scanner
    this.scanner = region.getScanner(scan);
    values = new ArrayList<KeyValue>();
    region.startRegionOperation();
  }

  @Override
  public Result next() throws IOException {
    values.clear();
    scanner.nextRaw(values, -1, null); // pass -1 as limit so that we see the whole row.
    if (values == null || values.isEmpty()) {
      //we are done
      return null;
    }
    return new Result(values);
  }

  @Override
  public void close() {
    if (this.scanner != null) {
      try {
        this.scanner.close();
        this.scanner = null;
      } catch (IOException ex) {
        Log.warn("Exception while closing scanner", ex);
      }
    }
    if (this.region != null) {
      try {
        this.region.closeRegionOperation();
        this.region.close(true);
        this.region = null;
      } catch (IOException ex) {
        Log.warn("Exception while closing region", ex);
      }
    }
  }

	@Override
	public Result[] next(int nbRows) throws IOException {
		throw new RuntimeException("Not Supported Yet");
	}
}