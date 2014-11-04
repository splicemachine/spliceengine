package com.splicemachine.mrio.sample;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLDouble;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.splicemachine.mrio.api.SpliceMRConstants;
import com.splicemachine.mrio.api.SpliceOutputFormat;
import com.splicemachine.mrio.api.SpliceTableMapReduceUtil;
import com.splicemachine.mrio.api.SpliceJob;
import com.splicemachine.mrio.api.SpliceInputFormat;

public class WordCount {
	static class MyMapper extends Mapper<ImmutableBytesWritable, ExecRow, Text, IntWritable>
	{
		
		private String word = "";
	    
		public void map(ImmutableBytesWritable row, ExecRow value, Context context) throws InterruptedException, IOException {
			
			if(value != null)
			{
				try {
					DataValueDescriptor dvd[]  = value.getRowArray();
					if(dvd[0] != null)
						{
						word = dvd[0].getString();
						}
					
				} catch (StandardException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if(word != null)
				{
					Text key = new Text(word.charAt(0)+"");
					IntWritable val = new IntWritable(1);
					context.write(key, val);
				}
			}	
		}
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, ImmutableBytesWritable, ExecRow> {
		
		  
		 @Override
		 public void reduce(Text key, Iterable<IntWritable> values, Context context)
		            throws IOException, InterruptedException {
		  
		  Iterator<IntWritable> it=values.iterator();
		  ExecRow execRow = new ValueRow(2);
		  int sum = 0;
		  String word = key.toString();
		  
		  while (it.hasNext()) {
		  
		   sum += it.next().get();
		  }
		  try{
			  DataValueDescriptor []dvds = {new SQLVarchar(word), new SQLInteger(sum)};
			  execRow.setRowArray(dvds);
			  context.write(new ImmutableBytesWritable(Bytes.toBytes(word)), execRow);
			  
		  }catch(Exception E)
		  {
			  E.printStackTrace();
		  }
		  
		 }
		}
	
	public static final String NAME = "WordCount";
	
	public static void main(String[] args) throws IOException {
		
		// We create a table first, then import a small dataset into it.
		//DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdbcType)
		
		// TODO Auto-generated method stub
		Configuration config = HBaseConfiguration.create();
		config.set(SpliceMRConstants.SPLICE_JDBC_STR, "jdbc:derby://localhost:1527/splicedb;user=splice;password=admin");
		
		SpliceJob job = new SpliceJob(config, NAME);
		
		job.setJarByClass(WordCount.class);     // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
	    
		String inputTableName = "WIKIDATA";
		String outputTableName = "USERTEST";
		
		try {
			SpliceTableMapReduceUtil.initTableMapperJob(
			inputTableName,        // input Splice table name
			scan,             // Scan instance to control CF and attribute selection
			MyMapper.class,   // mapper
			Text.class,       // mapper output key
			IntWritable.class,  // mapper output value
			job,
			true,
			SpliceInputFormat.class);
			
			SpliceTableMapReduceUtil.initTableReducerJob(
					outputTableName, 
					MyReducer.class, 
					job,
					null,
					null,
					null,
					null,
					false,
					SpliceOutputFormat.class);
			
			} catch (IOException e) {
			// TODO Auto-generated catch block
			    e.printStackTrace();
			} 
		
		boolean b = false;
		
			try {
				b = job.waitForCompletion(true);
				if (!b)
					{
					System.out.println("Job Failed");
					job.rollback();
					System.exit(1);
					}
				else{
					job.commit();
					System.out.println("Job Succeed");
					System.exit(0);
					
				}
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		  

}
