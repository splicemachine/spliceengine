/**
 * InputFormat which read rows from Splice and output ExecRow.
 *
 * @author Yanan Jian
 * Created on: 08/14/14
 */
package com.splicemachine.mrio.api;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;

public class SpliceInputFormat extends InputFormat<ImmutableBytesWritable, ExecRow> implements Configurable{

	private Configuration conf = null;
	private final Log LOG = LogFactory.getLog(TableInputFormat.class);
    private  static SpliceInputFormat inputFormat = null;
	private  HashMap<List, List> tableStructure = new HashMap<List, List>();
    private  SQLUtil sqlUtil = null;
    private  ArrayList<String> colNames = new ArrayList<String>();
    private  ArrayList<Integer>colTypes = new ArrayList<Integer>();

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
		tableName = configuration.get(SpliceMRConstants.SPLICE_INPUT_TABLE_NAME);	
		this.conf = configuration;
		try {
			
			if(sqlUtil == null)
				sqlUtil = SQLUtil.getInstance(conf.get(SpliceMRConstants.SPLICE_JDBC_STR));
			tableID = sqlUtil.getConglomID(tableName);
			conf.set(TableInputFormat.INPUT_TABLE, tableID);
			this.tableInputFormat.setConf(conf);
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
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
		// TODO Auto-generated method stub
		return tableInputFormat.getSplits(context);
		
	}
	
	public Scan getScan(){
		return tableInputFormat.getScan();
	}
}
