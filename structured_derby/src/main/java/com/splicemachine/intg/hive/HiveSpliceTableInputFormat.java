package com.splicemachine.intg.hive;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.splicemachine.mrio.api.SQLUtil;
import com.splicemachine.mrio.api.SpliceMRConstants;


public class HiveSpliceTableInputFormat extends SpliceTableInputFormatBase
										implements InputFormat<ImmutableBytesWritable, 
												ExecRowWritable>{

	private Configuration conf = null;
	private final Log LOG = LogFactory.getLog(HiveSpliceTableInputFormat.class);
    private  static HiveSpliceTableInputFormat inputFormat = null;
    private  SQLUtil sqlUtil = null;
    private  HiveSpliceRecordReader trr = null;
	//private  TableInputFormat tableInputFormat = new TableInputFormat();
    private HashMap<List, List> tableStructure = new HashMap<List, List>();
	private ArrayList<String> colNames = new ArrayList<String>();
    private ArrayList<Integer>colTypes = new ArrayList<Integer>();
	
	
	public static HiveSpliceTableInputFormat getInstance()
	{
		if(inputFormat == null)
			inputFormat = new HiveSpliceTableInputFormat();
		return inputFormat;
	}
	
	private String spliceTableName2HBaseTableName(String spliceTableName) throws IOException
	{
		String hbaseTableName = null;
		try {
			hbaseTableName = sqlUtil.getConglomID(spliceTableName);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			throw new IOException("Cannot find Conglomrate ID of Splice table:"+spliceTableName);
		}
		return hbaseTableName;
	}
	
	@Override
	public RecordReader<ImmutableBytesWritable, ExecRowWritable> 
											createRecordReader(org.apache.hadoop.mapreduce.InputSplit split, 
											TaskAttemptContext context) 
											throws IOException{
		if (trr == null) {
			
			trr = new HiveSpliceRecordReader(this.conf);
		}
		
		HTable table;
		try {
			String spliceTableName = conf.get(SpliceSerDe.SPLICE_TABLE_NAME).trim();
			tableStructure = sqlUtil.getTableStructure(spliceTableName);
			Iterator iter = tableStructure.entrySet().iterator();
	    	if(iter.hasNext())
	    	{
	    		Map.Entry kv = (Map.Entry)iter.next();
	    		colNames = (ArrayList<String>)kv.getKey();
	    		colTypes = (ArrayList<Integer>)kv.getValue();
	    	}
			String hbaseTableName = spliceTableName2HBaseTableName(spliceTableName);
			System.out.println("creating recordreader, Splice TableName:"+spliceTableName+
					           " hbase TableName:"+hbaseTableName);
			
			table = new HTable(HBaseConfiguration.create(conf), hbaseTableName);
			TableSplit tSplit = (TableSplit)split;
			trr.setHTable(table);
			trr.restart(tSplit.getStartRow());
			System.out.println("finished creating RecordReader");
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

	/**
	 * @param JobConf conf, int numSplits.
	 * To be compatible with mapred classes. 
	 * When running hive, it is called by server.
	 * Server can get accessed to all the lib/jar files.
	 * However when running query, the Tasktracker which actually executing the query 
	 * may not be able to get access to all the lib/jar files unless you explicitly add them 
	 * to hive aux lib path. eg. ./bin/hive --auxpath ($LIB_PATH)
	 * 
	 * Without adding aux lib path, if you are running SQL with 'where' clause, 
	 * you may see the error as : hcatalog.splice.SpliceSplit:Class hcatalog.splice.SpliceSplit not found
	 */
	@Override
	public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf jobConf,
			int numSplits) throws IOException {

			String spliceTableName = jobConf.get(SpliceSerDe.SPLICE_TABLE_NAME);
			if(sqlUtil == null)
				sqlUtil = SQLUtil.getInstance(jobConf.get(SpliceMRConstants.SPLICE_JDBC_STR));
			String hbaseTableName = spliceTableName2HBaseTableName(spliceTableName);
			setHTable(new HTable(HBaseConfiguration.create(jobConf), Bytes.toBytes(hbaseTableName)));
			Scan scan = new Scan();
			setScan(scan);
		    Job job = new Job(jobConf);
		    JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);
		    Path [] tablePaths = FileInputFormat.getInputPaths(jobContext);

		    List<org.apache.hadoop.mapreduce.InputSplit> splits =
		      super.getSplits(jobContext);
		    org.apache.hadoop.mapred.InputSplit [] results = new org.apache.hadoop.mapred.InputSplit[splits.size()];

		    for (int i = 0; i < splits.size(); i++) {
		      results[i] = new SpliceSplit((TableSplit) splits.get(i), tablePaths[0]);
		    }

		    return results;
		
	}

	/**
	 * @param mapred.InputSplit, JobConf, Reporter.
	 * To be compatible with mapred classes. 
	 */
	@Override
	public org.apache.hadoop.mapred.RecordReader<ImmutableBytesWritable, ExecRowWritable> getRecordReader(
			org.apache.hadoop.mapred.InputSplit split, JobConf jobConf,
			Reporter reporter) throws IOException {
		
		Job job = new Job(jobConf);
		
		this.conf = jobConf; 
		SpliceSplit hbaseSplit = (SpliceSplit) split;
		 TableSplit tableSplit = hbaseSplit.getSplit();
	    TaskAttemptContext tac = ShimLoader.getHadoopShims().newTaskAttemptContext(
	        job.getConfiguration(), reporter);
	    final org.apache.hadoop.mapreduce.RecordReader<ImmutableBytesWritable, ExecRowWritable>
	    recordReader = this.createRecordReader(tableSplit, tac);
	    return new org.apache.hadoop.mapred.RecordReader<ImmutableBytesWritable, ExecRowWritable>() {

	        @Override
	        public void close() throws IOException {
	          recordReader.close();
	        }

	        @Override
	        public ImmutableBytesWritable createKey() {
	          return new ImmutableBytesWritable();
	        }

	        @Override
	        public ExecRowWritable createValue() {
	          return new ExecRowWritable(colTypes);
	        }

	        @Override
	        public long getPos() throws IOException {
	          return 0;
	        }

	        @Override
	        public float getProgress() throws IOException {
	          float progress = 0.0F;

	          try {
	            progress = recordReader.getProgress();
	          } catch (InterruptedException e) {
	            throw new IOException(e);
	          }

	          return progress;
	        }

	        @Override
	        public boolean next(ImmutableBytesWritable rowKey, ExecRowWritable value) throws IOException {

	          boolean next = false;

	          try {
	            next = recordReader.nextKeyValue();
	            if (next) {
	              //rowKey.set(recordReader.getCurrentValue().get());
	              Writables.copyWritable(recordReader.getCurrentValue(), value);
	            }
	          } catch (InterruptedException e) {
	            throw new IOException(e);
	          }

	          return next;
	        }
	      };
	    
	}

}
