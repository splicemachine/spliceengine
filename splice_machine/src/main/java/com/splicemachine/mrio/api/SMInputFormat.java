package com.splicemachine.mrio.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.impl.job.scheduler.SubregionSplitter;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * 
 * Input Format that requires the following items passed to it.
 * 
 * 
 *
 */
public class SMInputFormat extends InputFormat<RowLocation, ExecRow> implements Configurable {
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
				conglomerate = util.getConglomID(tableName);
				conf.set(MRConstants.SPLICE_INPUT_CONGLOMERATE, conglomerate);
				} catch (SQLException e) {
					LOG.error(StringUtils.stringifyException(e));
					throw new RuntimeException(e);
				}		    	
		    }
		    try {
		      setHTable(new HTable(new Configuration(conf), conglomerate));
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
		conf.set(TableInputFormat.INPUT_TABLE,conf.get(MRConstants.SPLICE_INPUT_CONGLOMERATE));		
		tableInputFormat.setConf(conf);
		try {
			tableInputFormat.setScan(TableScannerBuilder.getTableScannerBuilderFromBase64String(conf.get(MRConstants.SPLICE_SCAN_INFO)).getScan());
		} catch (StandardException e) {			
			throw new IOException(e);
		}
		List<InputSplit> splits = tableInputFormat.getSplits(context);
		if (LOG.isTraceEnabled()) {
			SpliceLogUtils.debug(LOG, "getSplits");
			for (InputSplit split: splits) {
				SpliceLogUtils.debug(LOG, "split -> " + split);				
			}
		}
        SubregionSplitter splitter = DerbyFactoryDriver.derbyFactory.getSubregionSplitter();
        List<InputSplit> results = splitter.getSubSplits(table, splits);
		return results;
	}

	@Override
	public RecordReader<RowLocation, ExecRow> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "createRecordReader for split=%s, context %s",split,context);
		SMRecordReaderImpl recordReader = new SMRecordReaderImpl(context.getConfiguration());
		if(table == null)
			table = new HTable(HBaseConfiguration.create(conf), conf.get(MRConstants.SPLICE_INPUT_CONGLOMERATE));
		recordReader.setHTable(table);
//		recordReader.initialize((TableSplit) split, context);
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