package com.splicemachine.mrio.sample;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.splicemachine.mrio.api.SpliceInputFormat;
import com.splicemachine.mrio.api.SpliceJob;
import com.splicemachine.mrio.api.SpliceMRConstants;
import com.splicemachine.mrio.api.SpliceOutputFormat;
import com.splicemachine.mrio.api.SpliceTableMapReduceUtil;


public class WordCount2File {

	static class MyMapper extends Mapper<ImmutableBytesWritable, ExecRow, Text, Text>{
		Text val = new Text();
		public void map(ImmutableBytesWritable row, ExecRow value, Context context) throws InterruptedException, IOException {
			if(value != null)
			{
				String res = value.toString();
				String wt = res.substring(1, res.length()-2);
				Text key = new Text(wt);
				Text val = new Text("");
				context.write(key, val);			
			}
		}
	}
	
	public static final String NAME = "WordCountToCSV";
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration config = HBaseConfiguration.create();
		config.set(SpliceMRConstants.SPLICE_JDBC_STR, "jdbc:derby://localhost:1527/splicedb;user=splice;password=admin");
		//config.set("mapreduce.output.key.field.separator", ",");
		SpliceJob job = new SpliceJob(config, NAME);
		
		job.setJarByClass(WordCount.class);     // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
	    
		String inputTableName = "WIKIDATA";
		
		
		try {
			SpliceTableMapReduceUtil.initTableMapperJob(
			inputTableName,        // input Splice table name
			scan,             // Scan instance to control CF and attribute selection
			MyMapper.class,   // mapper
			Text.class,       // mapper output key
			Text.class,  // mapper output value
			job,
			true,
			SpliceInputFormat.class);
			
			} catch (IOException e) {
			// TODO Auto-generated catch block
			    e.printStackTrace();
			}
		/*SpliceTableMapReduceUtil.initTableReducerJob(
				outputTableName, 
				MyReducer.class, 
				job,
				null,
				null,
				null,
				null,
				false,
				TextOutputFormat.class);*/
		
		FileOutputFormat.setOutputPath(job, new Path("WIKIDATA"));
		job.setOutputFormatClass(TextOutputFormat.class);
		//job.setOutputFormatClass(NullOutputFormat.class);   // because we aren't emitting anything from mapper
		
		boolean b = false;
		
			try {
				b = job.waitForCompletion(true);
				if (!b)
					{
					System.out.println("Job Failed");
					job.rollback();
					}
				else{
					System.out.println("Job succeed");
					job.commit();
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


