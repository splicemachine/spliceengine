package com.splicemachine.mrio.api;

import java.io.IOException;
import java.util.List;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.SpliceMapreduceUtils;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class SMInputFormat extends InputFormat<ImmutableBytesWritable, ExecRow> implements Configurable {
    protected static final Logger LOG = Logger.getLogger(SMInputFormat.class);
	protected Configuration conf;
	protected HTable table;
	protected Scan scan;

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		    String tableName = conf.get(TableInputFormat.INPUT_TABLE);
		    try {
		      setHTable(new HTable(new Configuration(conf), tableName));
		    } catch (Exception e) {
		      LOG.error(StringUtils.stringifyException(e));
		    }

		    Scan scan = null;

		    if (conf.get(TableInputFormat.SCAN) != null) {
		      try {
		        scan = SpliceMapreduceUtils.convertStringToScan(conf.get(TableInputFormat.SCAN));
		      } catch (IOException e) {
		        LOG.error("An error occurred.", e);
		      }
		    } else {
		      try {
		        scan = new Scan();

		        if (conf.get(TableInputFormat.SCAN_ROW_START) != null) {
		          scan.setStartRow(Bytes.toBytes(conf.get(TableInputFormat.SCAN_ROW_START)));
		        }

		        if (conf.get(TableInputFormat.SCAN_ROW_STOP) != null) {
		          scan.setStopRow(Bytes.toBytes(conf.get(TableInputFormat.SCAN_ROW_STOP)));
		        }

		       // if (conf.get(TableInputFormat.SCAN_COLUMNS) != null) {
		       //   addColumns(scan, conf.get(TableInputFormat.SCAN_COLUMNS));
		       // }

		        if (conf.get(TableInputFormat.SCAN_COLUMN_FAMILY) != null) {
		          scan.addFamily(Bytes.toBytes(conf.get(TableInputFormat.SCAN_COLUMN_FAMILY)));
		        }

		        if (conf.get(TableInputFormat.SCAN_TIMESTAMP) != null) {
		          scan.setTimeStamp(Long.parseLong(conf.get(TableInputFormat.SCAN_TIMESTAMP)));
		        }

		        if (conf.get(TableInputFormat.SCAN_TIMERANGE_START) != null && conf.get(TableInputFormat.SCAN_TIMERANGE_END) != null) {
		          scan.setTimeRange(
		              Long.parseLong(conf.get(TableInputFormat.SCAN_TIMERANGE_START)),
		              Long.parseLong(conf.get(TableInputFormat.SCAN_TIMERANGE_END)));
		        }

		        if (conf.get(TableInputFormat.SCAN_MAXVERSIONS) != null) {
		          scan.setMaxVersions(Integer.parseInt(conf.get(TableInputFormat.SCAN_MAXVERSIONS)));
		        }

		        if (conf.get(TableInputFormat.SCAN_CACHEDROWS) != null) {
		          scan.setCaching(Integer.parseInt(conf.get(TableInputFormat.SCAN_CACHEDROWS)));
		        }

		        if (conf.get(TableInputFormat.SCAN_BATCHSIZE) != null) {
		          scan.setBatch(Integer.parseInt(conf.get(TableInputFormat.SCAN_BATCHSIZE)));
		        }

		        // false by default, full table scans generate too much BC churn
		        scan.setCacheBlocks((conf.getBoolean(TableInputFormat.SCAN_CACHEBLOCKS, false)));
		      } catch (Exception e) {
		          LOG.error(StringUtils.stringifyException(e));
		      }
		    }

		    setScan(scan);
		  }
		

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "getSplits");
		TableInputFormat tableInputFormat = new TableInputFormat();
		tableInputFormat.setConf(conf);
		return tableInputFormat.getSplits(context);
	}

	@Override
	public RecordReader<ImmutableBytesWritable, ExecRow> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		/*
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "createRecordReader for split=%s, context %s",split,context);
		SpliceTableRecordReader trr = this.spliceTableRecordReader;
		if(trr == null)
			trr = new SpliceTableRecordReader(conf);
		if(hTable == null)
			hTable = new HTable(HBaseConfiguration.create(conf), tableID);
		TableSplit tSplit = (TableSplit)split;
		Scan sc = new Scan(this.tableInputFormat.getScan());
		sc.setStartRow(tSplit.getStartRow());
		sc.setStopRow(tSplit.getEndRow());
		trr.setScan(sc);
		trr.setHTable(hTable);
		trr.init();
		return trr;
		*/
		return null;
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
	  
}