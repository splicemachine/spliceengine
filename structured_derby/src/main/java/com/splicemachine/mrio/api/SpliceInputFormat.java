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
   
    private  SpliceRecordReader trr = null;
	private  TableInputFormat tableInputFormat = new TableInputFormat();
	
	private SpliceInputFormat()
	{
		
		sqlUtil = SQLUtil.getInstance();

	}
	
	
	public static SpliceInputFormat getInstance()
	{
		if(inputFormat == null)
			inputFormat = new SpliceInputFormat();
		return inputFormat;
	}
	
	@Override
	public RecordReader<ImmutableBytesWritable, ExecRow> 
											createRecordReader(InputSplit split, 
											TaskAttemptContext context) 
											throws IOException{
		if (trr == null) {
			
			trr = new SpliceRecordReader(this.conf);
		}
		
		HTable table;
		try {
			
			table = new HTable(HBaseConfiguration.create(conf), tableID);
			TableSplit tSplit = (TableSplit)split;
			trr.setHTable(table);
			trr.restart(tSplit.getStartRow());
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return trr;
	}
	
	static Scan convertStringToScan(String base64) throws IOException {
		    ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decode(base64));
		    DataInputStream dis = new DataInputStream(bis);
		    Scan scan = new Scan();
		    scan.readFields(dis);
		    return scan;
    }
	
	public Configuration getConf() {
		return this.conf;
	}
	
	
	
	public void setConf(Configuration configuration) {
		
		tableName = configuration.get(SpliceMRConstants.SPLICE_INPUT_TABLE_NAME);		
		try {
			tableID = sqlUtil.getConglomID(tableName);
			this.conf = configuration;
			conf.set(TableInputFormat.INPUT_TABLE, tableID);
			
			
			this.tableInputFormat.setConf(conf);
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		// TODO Auto-generated method stub
		return tableInputFormat.getSplits(context);
		
	}

}
