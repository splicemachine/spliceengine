package com.splicemachine.intg.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.splicemachine.mrio.api.SpliceTableRecordReader;

public class HiveSpliceRecordReader extends RecordReader<ImmutableBytesWritable, ExecRowWritable>{

	private SpliceTableRecordReader recordReader = null;
	private List<Integer> colTypes;
	private Configuration conf = null;
	private HashMap<List, List> tableStructure = null;
	
	public void setConf(Configuration conf){
		
		this.conf = conf;
	}
	
	public void setScan(Scan scan) throws IOException{
		if(recordReader == null){
			if(conf != null)
				recordReader = new SpliceTableRecordReader(conf);
			else
				throw new IOException("Haven't set Conf for HiveSpliceRecordReader2, "
										+ "can't initialize RecordReader");
		}
	    this.recordReader.setScan(scan);
	  }
	
	public void setHTable(HTable htable) throws IOException{
		if(recordReader == null){
			if(conf != null)
				recordReader = new SpliceTableRecordReader(conf);
			else
				throw new IOException("Haven't set Conf for HiveSpliceRecordReader2, "
										+ "can't initialize RecordReader");
		}
	    this.recordReader.setHTable(htable);
	  }
	
	public void init() throws IOException{
		if(recordReader == null){
			if(conf != null)
				recordReader = new SpliceTableRecordReader(conf);
			else
				throw new IOException("Haven't set Conf for HiveSpliceRecordReader2, "
										+ "can't initialize RecordReader");
		}
		this.recordReader.init();
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(recordReader == null){
			if(conf != null)
				recordReader = new SpliceTableRecordReader(conf);
			else
				throw new IOException("Haven't set Conf for HiveSpliceRecordReader2, "
										+ "can't initialize RecordReader");
		}
			
		
		recordReader.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return recordReader.nextKeyValue();
	}

	@Override
	public ImmutableBytesWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return recordReader.getCurrentKey();
	}

	@Override
	public ExecRowWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		ExecRow execRow = recordReader.getCurrentValue();
		
		// Initialize colTypes first!
		
		ExecRowWritable res = new ExecRowWritable(colTypes);
		res.set(execRow);
		return res;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return recordReader.getProgress();
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		recordReader.close();
	}
	
	public void setTableStructure(HashMap<List, List> tableStructure){
		this.tableStructure = tableStructure;
		Iterator iter = tableStructure.entrySet().iterator();
    	if(iter.hasNext())
    	{
    		Map.Entry kv = (Map.Entry)iter.next();
    		colTypes = (ArrayList<Integer>)kv.getValue();
    	}
	}

}
