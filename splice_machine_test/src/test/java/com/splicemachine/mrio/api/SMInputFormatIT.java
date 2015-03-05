package com.splicemachine.mrio.api;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.util.List;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;

import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import scala.Tuple2;

import com.splicemachine.derby.test.framework.SpliceSparkWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.mrio.MRConstants;

public class SMInputFormatIT extends BaseMRIOTest {	
	private static final String CLASS_NAME = SMInputFormatIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);
    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static final SpliceTableWatcher tableWatcherA = new SpliceTableWatcher(
    	"A", schemaWatcher.schemaName, "(col1 varchar(100) primary key, col2 varchar(100))");
    private static final SpliceTableWatcher tableWatcherB = new SpliceTableWatcher(
        	"B", schemaWatcher.schemaName, "(col1 bigint primary key, col2 varchar(100))");
    private static final SpliceSparkWatcher sparkWatcher = new SpliceSparkWatcher(CLASS_NAME);
       
    
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(tableWatcherA)
            .around(tableWatcherB)
            .around(sparkWatcher)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try{
                        PreparedStatement ps1;
                        PreparedStatement ps2;
                        ps1 = classWatcher.prepareStatement(
							"insert into " + tableWatcherA + " (col1, col2) values (?,?)");
                        ps1.setString(1, "John");
                        ps1.setString(2, "Leach");
                        ps1.executeUpdate();
                        ps1.setString(1, "Jenny");
                        ps1.setString(2, "Swift");
                        ps1.executeUpdate();
    
                        ps2 = classWatcher.prepareStatement(
							"insert into " + tableWatcherB + " (col1, col2) values (?,?)");
                        for (int i =0; i<10000;i++) {
                        	ps2.setInt(1, i);
                        	ps2.setString(2, i+"sdfasdgffdgdfgdfgdfgdfgdfgdfgd");
                        	ps2.executeUpdate();     
                        	if (i==500) {
                        		flushTable(tableWatcherB.toString());
                        		splitTable(tableWatcherB.toString());                        		
                        	}
                        	if (i==750) {
                        		flushTable(tableWatcherB.toString());
                        	}

                        	if (i==1000) {
                        		flushTable(tableWatcherB.toString());
                        	}

                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        classWatcher.closeAll();
                    }
                }
            });
   
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);
	
    
    
    @Test
    @Ignore
    public void testSparkIntegrationWithInputFormat() throws IOException {
    	config.set(MRConstants.SPLICE_JDBC_STR, "jdbc:derby://localhost:1527/splicedb;user=splice;password=admin");
    	config.set(MRConstants.SPLICE_INPUT_TABLE_NAME, tableWatcherA.toString());
    	Job job = new Job(config, "Test Scan");	
        JavaPairRDD<RowLocation, ExecRow> table = sparkWatcher.jsc.newAPIHadoopRDD(job.getConfiguration(), SMInputFormat.class, RowLocation.class, ExecRow.class);
        List<Tuple2<RowLocation, ExecRow>> data = table.collect();
        int i = 0;
        for (Tuple2<RowLocation, ExecRow> tuple: data) {
        	i++;
        	Assert.assertNotNull(tuple._1);
        	Assert.assertNotNull(tuple._2);        	
        }
        Assert.assertEquals("Incorrect Results Returned", 2,i);   	
    }
    
    @Test
    @Ignore
    public void testCountOverMultipleRegionsInSpark() throws IOException {
    	config.set(MRConstants.SPLICE_JDBC_STR, "jdbc:derby://localhost:1527/splicedb;user=splice;password=admin");
    	config.set(MRConstants.SPLICE_INPUT_TABLE_NAME, tableWatcherB.toString());
    	Job job = new Job(config, "Test Scan");	
        JavaPairRDD<RowLocation, ExecRow> table = sparkWatcher.jsc.newAPIHadoopRDD(job.getConfiguration(), SMInputFormat.class, RowLocation.class, ExecRow.class);
        List<Tuple2<RowLocation, ExecRow>> data = table.collect();
        int i = 0;
        for (Tuple2<RowLocation, ExecRow> tuple: data) {
        	i++;
        	Assert.assertNotNull(tuple._1);
        	Assert.assertNotNull(tuple._2);        	
        }
        Assert.assertEquals("Incorrect Results Returned", 10000,i);
    }
    
}
