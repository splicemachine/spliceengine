package com.splicemachine.mrio.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import com.splicemachine.mrio.MRConstants;
import com.splicemachine.utils.SpliceLogUtils;

public class SMInputFormat extends InputFormat<ImmutableBytesWritable, ExecRow> implements Configurable {
    protected static final Logger LOG = Logger.getLogger(SMInputFormat.class);
	protected Configuration conf;
	protected HTable table;
	protected Scan scan;
	protected SMSQLUtil util;

	@Override
	public void setConf(Configuration conf) {
		    String tableName = conf.get(MRConstants.SPLICE_INPUT_TABLE_NAME);
		    String conglomerate = conf.get(MRConstants.SPLICE_INPUT_CONGLOMERATE);
			String tableScannerAsString = conf.get(MRConstants.SPLICE_SCAN_INFO);
			String jdbcString = conf.get(MRConstants.SPLICE_JDBC_STR);
			if (tableName == null && conglomerate == null) {
			    LOG.error("Table Name Supplied is null");
		    	throw new RuntimeException("Table Name Supplied is Null");
		    }
		    if (conglomerate == null) {
				if (util==null)
					util = SMSQLUtil.getInstance(jdbcString);
				if (jdbcString == null) {
					LOG.error("JDBC String Not Supplied");
					throw new RuntimeException("JDBC String Not Supplied");
				}
				try {
				tableName = util.getConglomID(tableName);
				} catch (SQLException e) {
					LOG.error(StringUtils.stringifyException(e));
					throw new RuntimeException(e);
				}		    	
		    }
		    try {
		      setHTable(new HTable(new Configuration(conf), tableName));
		    } catch (Exception e) {
		      LOG.error(StringUtils.stringifyException(e));
		    }
			if (tableScannerAsString == null) {
				if (jdbcString == null) {
					LOG.error("JDBC String Not Supplied");
					throw new RuntimeException("JDBC String Not Supplied");
				}				
				try {
					conf.set(MRConstants.SPLICE_SCAN_INFO, util.getTableScannerBuilder(tableName, null).getTableScannerBuilderBase64String());
				} catch (Exception e) {
					LOG.error(StringUtils.stringifyException(e));
					throw new RuntimeException(e);
				}
			}		    
		    this.conf = conf;
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
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "createRecordReader for split=%s, context %s",split,context);
		SMRecordReaderImpl recordReader = new SMRecordReaderImpl(context.getConfiguration());
		if(table == null)
			table = new HTable(HBaseConfiguration.create(conf), conf.get(MRConstants.SPLICE_INPUT_TABLE_NAME));
		recordReader.setHTable(table);
		recordReader.initialize((TableSplit) split, context);
		return recordReader;
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
	  
}