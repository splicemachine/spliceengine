package com.splicemachine.mrio;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.DefaultedSpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.mrio.api.SpliceInputFormat;
import com.splicemachine.mrio.api.SpliceJob;
import com.splicemachine.mrio.api.SpliceMRConstants;
import com.splicemachine.mrio.api.SpliceOutputFormat;
import com.splicemachine.mrio.api.SpliceTableMapReduceUtil;
import com.splicemachine.mrio.sample.WordCount;

public class MapReduceIT {

    private static final String CLASS_NAME = MapReduceIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher classWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    // Table for ADD_MONTHS testing.
    private static final SpliceTableWatcher tableWatcherA = new SpliceTableWatcher(
    	"A", schemaWatcher.schemaName, "(col1 int, col2 int)");
   
    
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(tableWatcherA)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try{
                        PreparedStatement ps;
                        
                        // Each of the following inserted rows represents an individual test,
                        // including expected result (column 'col3'), for less test code in the
                        // test methods

                        ps = classWatcher.prepareStatement(
							"insert into " + tableWatcherA + " (col1, col2) values (1,2)");
                        ps = classWatcher.prepareStatement(
    							"insert into " + tableWatcherA + " (col1, col2) values (3,4)");
                        ps = classWatcher.prepareStatement(
    							"insert into " + tableWatcherA + " (col1, col2) values (5,6)");
                      
                        ps.execute();
                        
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        classWatcher.closeAll();
                    }
                }
            });
   
    @Rule
    public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    @Test
    public void testMapper() throws Exception{
		 class MyMapper extends Mapper<ImmutableBytesWritable, ExecRow, Text, IntWritable>{
 			
			private String word = "";
			
			public void map(ImmutableBytesWritable row, ExecRow value, Context context) throws InterruptedException, IOException {
				
				if(value != null){
					try {
						DataValueDescriptor dvd[]  = value.getRowArray();
						if(dvd[0] != null)
							{
							word = String.valueOf(dvd[0].getInt());
							System.out.println(word);
							}
						
					} catch (StandardException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					if(word != null){
						Text key = new Text(word.charAt(0)+"");
						IntWritable val = new IntWritable(1);
						context.write(key, val);
					}
				}
			}
		}
    	Configuration config = HBaseConfiguration.create();
 		config.set(SpliceMRConstants.SPLICE_JDBC_STR, "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin");
 		SpliceJob job = new SpliceJob(config, "MRIT");
 		
 		job.setJarByClass(WordCount.class);     // class that contains mapper

 		Scan scan = new Scan();
 		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
 		scan.setCacheBlocks(false);  // don't set to true for MR jobs
 	    
 		String inputTableName = "MAPREDUCEIT.A";
 		
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
 			//FileOutputFormat.setOutputPath(job, new Path("WIKIDATA"));
 			
 			} catch (IOException e) {
 			// TODO Auto-generated catch block
 			    e.printStackTrace();
 			} 
 		job.setOutputFormatClass(NullOutputFormat.class);
 		boolean b = false;
		
		try {
			b = job.waitForCompletion(true);
			if (!b)
				{			
				System.out.println("Job Failed");
				job.rollback();
				}
			else
				job.commit();
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
    
    	/*ResultSet rs;
    	rs = methodWatcher.executeQuery("SELECT TO_DATE(col1, col2), col3 from " + tableWatcherA);
    	while(rs.next()){
    		Assert.assertEquals(rs.getDate(2), rs.getDate(1));
    	}*/
    }

   
}

