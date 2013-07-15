package com.splicemachine.spark;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

import spark.api.java.function.FlatMapFunction;
import spark.api.java.function.Function2;
import spark.api.java.function.PairFunction;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import com.splicemachine.derby.utils.SpliceUtils;

import scala.Tuple2;
import spark.api.java.JavaPairRDD;
import spark.api.java.JavaSparkContext;
import spark.broadcast.Broadcast;

public class SpliceSparkContext {
	protected static JavaSparkContext ctx;
	protected static String[] jars = {"/Users/johnleach/.m2/repository/org/apache/derby/derby/10.9.1.0.splice/derby-10.9.1.0.splice.jar","/Users/johnleach/splicemachine/workspace/structured_hbase/structured_derby/target/splice_machine-1.0.1-SNAPSHOT.jar","/Users/johnleach/.m2/repository/org/apache/hbase/hbase/0.94.6-cdh4.3.0/hbase-0.94.6-cdh4.3.0.jar","/Users/johnleach/splicemachine/workspace/structured_hbase/structured_spark/target/structured_spark-1.0.1-SNAPSHOT.jar"};

    private SpliceSparkContext() {
    	//ctx = new JavaSparkContext("local", "JavaWordCount");
    	    	ctx = new JavaSparkContext("spark://Johns-MacBook-Pro-2.local:7077", "JavaWordCount", "/Users/johnleach/SpliceMachine/spark-0.7.2",jars);
    }
    
    public static JavaSparkContext getSparkContext() {
    	if (ctx == null)
    		new SpliceSparkContext();
    	return ctx;
    }
 
    /**
     * Broadcasts the SpliceObserverInstructions around the cluster.
     * 
     * @param instructions
     * @return
     */
    public static Broadcast<SpliceObserverInstructions> broadCastVariable(SpliceObserverInstructions instructions) {
    	return ctx.broadcast(instructions);
    }

    
    /**
     * Generate RDD from Durable Storage
     * 
     */
    public static JavaPairRDD<RowLocation, ExecRow> generateDurableStorageRDD(Scan scan, SpliceObserverInstructions instructions, HTableInterface table) {
        if(scan.getAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS)==null)
            SpliceUtils.setInstructions(scan,instructions);
    	// Need to add transactional information...
        Configuration conf = HBaseConfiguration.create();
	    return ctx.newAPIHadoopRDD(conf, SpliceInputFormat.class, RowLocation.class, ExecRow.class);	    
    }

    /**
     * Generate RDD from Transient / In Memory Storage
     * 
     */
/*    public static JavaPairRDD<RowLocation, ExecRow> generateDurableStorageRDD() {
	    Configuration conf = HBaseConfiguration.create();
	    return ctx.newAPIHadoopRDD(conf, SpliceInputFormat.class, RowLocation.class, ExecRow.class);	    
    }    
 */   
    /**
     * Code for Aggregations
     * 
     * @param pair
     */
    public static JavaPairRDD<RowLocation,ExecRow> applyReduceFunction(JavaPairRDD<RowLocation, ExecRow> row,final Broadcast<SpliceObserverInstructions> instructions) {    	
    	return row.reduceByKey(new Function2<ExecRow,ExecRow,ExecRow>() {
			@Override
			public ExecRow call(ExecRow row1, ExecRow row2) throws Exception {
				
				return null;
			}
    		
    	});
    }

    
    public static JavaPairRDD<RowLocation,ExecRow> applyMapFunction(JavaPairRDD<RowLocation, ExecRow> row, final Broadcast<SpliceObserverInstructions> instructions) {
    	
    	return row.map(new PairFunction<Tuple2<RowLocation,ExecRow>,RowLocation,ExecRow>() {
			@Override
			public Tuple2<RowLocation, ExecRow> call(Tuple2<RowLocation, ExecRow> source) throws Exception {
				
				// TODO Auto-generated method stub
				return null;
			}

    	});
    }
}
