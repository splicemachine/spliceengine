package com.splicemachine.intg.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

public abstract class SpliceTableInputFormatBase extends InputFormat<ImmutableBytesWritable, 
ExecRowWritable>{
	/** Holds the details for the internal scanner. */
	  private Scan scan = null;
	  /** The table to scan. */
	  private HTable table = null;
	  /** The reader scanning the table, can be a custom one. */
	  private TableRecordReader tableRecordReader = null;
	  static Logger Log = Logger.getLogger(
			  SpliceTableInputFormatBase.class.getName());
	  
	@Override
	public RecordReader<ImmutableBytesWritable, ExecRowWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException { 
		    return null;
	}

	@Override
	  public List<InputSplit> getSplits(JobContext context) throws IOException {
		if (table == null) {
		    throw new IOException("No table was provided.");
		}
	    Pair<byte[][], byte[][]> keys = table.getStartEndKeys();
	    if (keys == null || keys.getFirst() == null ||
	        keys.getFirst().length == 0) {
	      throw new IOException("Expecting at least one region.");
	    }
	    int count = 0;
	    List<InputSplit> splits = new ArrayList<InputSplit>(keys.getFirst().length);
	    for (int i = 0; i < keys.getFirst().length; i++) {
	      if ( !includeRegionInSplit(keys.getFirst()[i], keys.getSecond()[i])) {
	        continue;
	      }
	      String regionLocation = table.getRegionLocation(keys.getFirst()[i]).
	        getServerAddress().getHostname();
	      byte[] startRow = scan.getStartRow();
	      byte[] stopRow = scan.getStopRow();
	      // determine if the given start an stop key fall into the region
	      if ((startRow.length == 0 || keys.getSecond()[i].length == 0 ||
	           Bytes.compareTo(startRow, keys.getSecond()[i]) < 0) &&
	          (stopRow.length == 0 ||
	           Bytes.compareTo(stopRow, keys.getFirst()[i]) > 0)) {
	        byte[] splitStart = startRow.length == 0 ||
	          Bytes.compareTo(keys.getFirst()[i], startRow) >= 0 ?
	            keys.getFirst()[i] : startRow;
	        byte[] splitStop = (stopRow.length == 0 ||
	          Bytes.compareTo(keys.getSecond()[i], stopRow) <= 0) &&
	          keys.getSecond()[i].length > 0 ?
	            keys.getSecond()[i] : stopRow;
	        InputSplit split = new TableSplit(table.getTableName(),
	          splitStart, splitStop, regionLocation);
	        splits.add(split);
	        if (Log.isDebugEnabled())
	          Log.debug("getSplits: split -> " + (count++) + " -> " + split);
	      }
	    }
	    return splits;
	  }

	  /**
	   *
	   *
	   * Test if the given region is to be included in the InputSplit while splitting
	   * the regions of a table.
	   * <p>
	   * This optimization is effective when there is a specific reasoning to exclude an entire region from the M-R job,
	   * (and hence, not contributing to the InputSplit), given the start and end keys of the same. <br>
	   * Useful when we need to remember the last-processed top record and revisit the [last, current) interval for M-R processing,
	   * continuously. In addition to reducing InputSplits, reduces the load on the region server as well, due to the ordering of the keys.
	   * <br>
	   * <br>
	   * Note: It is possible that <code>endKey.length() == 0 </code> , for the last (recent) region.
	   * <br>
	   * Override this method, if you want to bulk exclude regions altogether from M-R. By default, no region is excluded( i.e. all regions are included).
	   *
	   *
	   * @param startKey Start key of the region
	   * @param endKey End key of the region
	   * @return true, if this region needs to be included as part of the input (default).
	   *
	   */
	  protected boolean includeRegionInSplit(final byte[] startKey, final byte [] endKey) {
	    return true;
	  }

	  /**
	   * Allows subclasses to get the {@link HTable}.
	   */
	  protected HTable getHTable() {
	    return this.table;
	  }

	  /**
	   * Allows subclasses to set the {@link HTable}.
	   *
	   * @param table  The table to get the data from.
	   */
	  protected void setHTable(HTable table) {
	    this.table = table;
	  }

	  /**
	   * Gets the scan defining the actual details like columns etc.
	   *
	   * @return The internal scan instance.
	   */
	  public Scan getScan() {
	    if (this.scan == null) this.scan = new Scan();
	    return scan;
	  }

	  /**
	   * Sets the scan defining the actual details like columns etc.
	   *
	   * @param scan  The scan to set.
	   */
	  public void setScan(Scan scan) {
	    this.scan = scan;
	  }

	  /**
	   * Allows subclasses to set the {@link TableRecordReader}.
	   *
	   * @param tableRecordReader A different {@link TableRecordReader}
	   *   implementation.
	   */
	  protected void setTableRecordReader(TableRecordReader tableRecordReader) {
	    this.tableRecordReader = tableRecordReader;
	  }
	
}
