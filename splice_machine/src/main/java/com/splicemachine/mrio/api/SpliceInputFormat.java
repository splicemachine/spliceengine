/**
 * InputFormat which read rows from Splice and output ExecRow.
 *
 * @author Yanan Jian
 * Created on: 08/14/14
 */
package com.splicemachine.mrio.api;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
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
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

public class SpliceInputFormat extends InputFormat<ImmutableBytesWritable, ExecRow> implements Configurable{
    protected static final Logger LOG = Logger.getLogger(SpliceInputFormat.class);
	private Configuration conf = null;
    private  SQLUtil sqlUtil = null;
    private  String tableID = null;
    private  String tableName = null;
    private  SpliceTableRecordReader spliceTableRecordReader = null;
	private  TableInputFormat tableInputFormat = new TableInputFormat();
	private  HTable hTable = null;
	
	/**
	 * Used by InputFormat framework, do not call this method.
	 */
	@Override
	public RecordReader<ImmutableBytesWritable, ExecRow> 
											createRecordReader(InputSplit split, 
											TaskAttemptContext context) 
											throws IOException{
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
	}
	
	/**
	 * get Configuration of this InputFormat
	 * @return org.apache.hadoop.conf.Configuration
	 */
	public Configuration getConf() {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "getConf");
		return this.tableInputFormat.getConf();
	}
	
	
	/**
	 * 
	 * Set configuration for InputFormat
	 * Should set SpliceMRConstants.SPLICE_INPUT_TABLE_NAME in the Configuration.
	 * @param Configuration
	 * 
	 */
	public void setConf(Configuration configuration) {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "setConf");
		tableName = configuration.get(SpliceMRConstants.SPLICE_INPUT_TABLE_NAME);	
		this.conf = configuration;
		try {
			if(sqlUtil == null)
				sqlUtil = SQLUtil.getInstance(conf.get(SpliceMRConstants.SPLICE_JDBC_STR));
			tableID = sqlUtil.getConglomID(tableName);
			conf.set(TableInputFormat.INPUT_TABLE, tableID);
			this.tableInputFormat.setConf(conf);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		
	}

	/**
	 * Used by InputFormat framework, get List of Map Reduce InputSplit
	 * @param org.apache.hadoop.mapreduce.JobContext
	 * @return List<org.apache.hadoop.mapreduce.InputSplit>
	 */
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "getSplits with context=%s",context);
		if (tableInputFormat.getScan() == null)
			tableInputFormat.setScan(new Scan());
		return tableInputFormat.getSplits(context);
		
	}
	
	public Scan getScan(){
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "getScan");
		return tableInputFormat.getScan();
	}
}
