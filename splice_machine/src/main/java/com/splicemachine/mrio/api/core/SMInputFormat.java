package com.splicemachine.mrio.api.core;

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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
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
	protected SMRecordReaderImpl rr;
	protected boolean spark;

	@Override
	public void setConf(Configuration conf) {
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "setConf conf=%s",conf);
		    String tableName = conf.get(MRConstants.SPLICE_TABLE_NAME);
		    String conglomerate = conf.get(MRConstants.SPLICE_CONGLOMERATE);
			String tableScannerAsString = conf.get(MRConstants.SPLICE_SCAN_INFO);
			spark = tableScannerAsString!=null;
			conf.setBoolean("splice.spark", spark);
			String jdbcString = conf.get(MRConstants.SPLICE_JDBC_STR);
			String rootDir = conf.get(HConstants.HBASE_DIR);
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "setConf tableName=%s, conglomerate=%s, tableScannerAsString=%s"
						+ "jdbcString=%s, rootDir=%s",tableName,conglomerate,tableScannerAsString,jdbcString, rootDir);

			
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
				conf.set(MRConstants.SPLICE_CONGLOMERATE, conglomerate);
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
			if (LOG.isTraceEnabled())
				SpliceLogUtils.trace(LOG, "finishingSetConf");
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
			SpliceLogUtils.debug(LOG, "getSplits with context=%s",context);
		TableInputFormat tableInputFormat = new TableInputFormat();
		conf.set(TableInputFormat.INPUT_TABLE,conf.get(MRConstants.SPLICE_CONGLOMERATE));		
		tableInputFormat.setConf(conf);
		try {
			tableInputFormat.setScan(TableScannerBuilder.getTableScannerBuilderFromBase64String(conf.get(MRConstants.SPLICE_SCAN_INFO)).getScan());
		} catch (StandardException e) {
			SpliceLogUtils.error(LOG, e);
			throw new IOException(e);
		}
		List<InputSplit> splits = tableInputFormat.getSplits(context);
		if (LOG.isDebugEnabled()) {
			SpliceLogUtils.debug(LOG, "getSplits " + splits);
			for (InputSplit split: splits) {
				SpliceLogUtils.debug(LOG, "split -> " + split);				
			}
		}
        SubregionSplitter splitter = DerbyFactoryDriver.derbyFactory.getSubregionSplitter();
        List<InputSplit> results = splitter.getSubSplits(table, splits);
		return results;
	}

	public SMRecordReaderImpl getRecordReader(InputSplit split, Configuration config) throws IOException,
	InterruptedException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "getRecorderReader with table=%s, conglomerate",table,config.get(MRConstants.SPLICE_CONGLOMERATE));		
		rr = new SMRecordReaderImpl(config);
		if(table == null)
			table = new HTable(HBaseConfiguration.create(config), config.get(MRConstants.SPLICE_CONGLOMERATE));
		rr.setHTable(table);
		if (!conf.getBoolean("splice.spark", false))
			rr.init(config, split);
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "returning record reader");		
		return rr;
	}
	
	@Override
	public RecordReader<RowLocation, ExecRow> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "createRecordReader for split=%s, context %s",split,context);
		if (rr != null)
			return rr;
		return getRecordReader(split,context.getConfiguration());
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