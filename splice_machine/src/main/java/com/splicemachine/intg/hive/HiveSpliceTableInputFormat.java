package com.splicemachine.intg.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.splicemachine.mrio.api.SQLUtil;
import com.splicemachine.mrio.api.SpliceInputFormat;
import com.splicemachine.mrio.api.SpliceTableRecordReader;

public class HiveSpliceTableInputFormat extends InputFormat<ImmutableBytesWritable, 
ExecRowWritable> implements org.apache.hadoop.mapred.InputFormat<ImmutableBytesWritable, 
ExecRowWritable>{

	SpliceInputFormat inputFormat = new SpliceInputFormat();
	HiveSpliceRecordReader spliceTableRecordReader = null;
	Configuration conf = null;
	private  static SQLUtil sqlUtil = null;
    private  HiveSpliceRecordReader trr = null;
    private List<Integer> colTypes = null;
    private List<String> allColNames = null;
    private HashMap<List, List> tableStructure = new HashMap<>();
	private Connection parentConn = null;
 
	
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	private String spliceTableName2HBaseTableName(String spliceTableName) throws IOException{
		String hbaseTableName = null;
		try {
			hbaseTableName = sqlUtil.getConglomID(spliceTableName);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			throw new IOException(e);
		}
		return hbaseTableName;
	}
	
	
	@Override
	public RecordReader<ImmutableBytesWritable, ExecRowWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		String spliceTableName = conf.get(SpliceSerDe.SPLICE_INPUT_TABLE_NAME);
		HiveSpliceRecordReader trr = this.spliceTableRecordReader;
		
		sqlUtil = SQLUtil.getInstance(conf.get(SpliceSerDe.SPLICE_JDBC_STR));
		String txnId = sqlUtil.getTransactionID();
		if(conf.get(SpliceSerDe.SPLICE_TRANSACTION_ID) == null) {
            conf.set(SpliceSerDe.SPLICE_TRANSACTION_ID, txnId);
        }
		trr = new HiveSpliceRecordReader();
		trr.setConf(conf);
			
		tableStructure = sqlUtil.getTableStructure(spliceTableName);
			
		Iterator iter = tableStructure.entrySet().iterator();
	    	if(iter.hasNext())
	    	{
	    		Map.Entry kv = (Map.Entry)iter.next();
	    		allColNames = (ArrayList<String>)kv.getKey(); 
	    		colTypes = (ArrayList<Integer>)kv.getValue();
	    	}
	    	
		//}
		String hbaseTableName = spliceTableName2HBaseTableName(spliceTableName);
		HTable hTable = new HTable(HBaseConfiguration.create(conf), hbaseTableName);
		TableSplit tSplit = (TableSplit)split;
		//Scan sc = new Scan(this.inputFormat.getScan());
		Scan sc = new Scan();
		sc.setStartRow(tSplit.getStartRow());
		sc.setStopRow(tSplit.getEndRow());
		
		trr.setScan(sc);
		trr.setHTable(hTable);
		trr.init();
		trr.setTableStructure(tableStructure);
		
		return trr;
	}

	@Override
	public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf jobConf,
			int numSplits) throws IOException {
		Job job = new Job(jobConf);
		//if(this.conf == null)
		this.conf = jobConf;
	    JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);
	    inputFormat.setConf(conf);
	    List<InputSplit> splits = inputFormat.getSplits(jobContext);
	    
	    org.apache.hadoop.mapred.InputSplit [] results = new org.apache.hadoop.mapred.InputSplit[splits.size()];
	    Path [] tablePaths = FileInputFormat.getInputPaths(jobContext);
	    for (int i = 0; i < splits.size(); i++) {
	      results[i] = new SpliceSplit((TableSplit) splits.get(i), tablePaths[0]);
	    }

	    return results;
	}

	@Override
	public org.apache.hadoop.mapred.RecordReader<ImmutableBytesWritable, ExecRowWritable> getRecordReader(
			org.apache.hadoop.mapred.InputSplit split, JobConf jobConf,
			Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		Job job = new Job(jobConf);
		//if(this.conf == null){
		this.conf = jobConf;
		//}
			
		SpliceSplit hbaseSplit = (SpliceSplit) split;
		 TableSplit tableSplit = hbaseSplit.getSplit();
	    TaskAttemptContext tac = ShimLoader.getHadoopShims().newTaskAttemptContext(
	        job.getConfiguration(), reporter);
	    
		try {
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
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			throw new IOException("Interrupted ");
		}
		
	}

}
