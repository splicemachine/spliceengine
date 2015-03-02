package org.apache.spark.examples;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.splicemachine.mrio.api.SMInputFormat;
import com.splicemachine.mrio.api.SMSQLUtil;
import com.splicemachine.mrio.api.SpliceJob;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.SpliceTableMapReduceUtil;

/**
 * Computes an approximation to pi
 * Usage: JavaSparkPi [slices]
 */
public final class JavaSparkPI {

  public static void main(String[] args) throws Exception {
	  	
	
	long beginTime = System.currentTimeMillis();	
	Configuration config = HBaseConfiguration.create();
	config.set(MRConstants.SPLICE_JDBC_STR, "jdbc:derby://localhost:1527/splicedb;user=splice;password=admin");


	SMSQLUtil sqlUtil = SMSQLUtil.getInstance(config.get(MRConstants.SPLICE_JDBC_STR));
	String txsID = sqlUtil.getTransactionID();
	
	config.set(MRConstants.SPLICE_TRANSACTION_ID, txsID);
	SpliceJob job = new SpliceJob(config, "Test Scan");	
	job.setJarByClass(JavaSparkPI.class);     // class that contains mapper
	Scan scan = new Scan();
	scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
	scan.setCacheBlocks(false);  // don't set to true for MR jobs		
	String inputTableName = "FOO";
	
	
	String foo = SpliceTableMapReduceUtil.convertScanToString(scan);
	SpliceTableMapReduceUtil.convertStringToScan(foo);
	
		SpliceTableMapReduceUtil.initTableMapperJob(
		inputTableName,        // input Splice table name
		scan,             // Scan instance to control CF and attribute selection
		null,   // mapper
		IntWritable.class,       // mapper output key
		ExecRow.class,  // mapper output value
		job,
		true,
		SMInputFormat.class);
		job.getConfiguration().set(TableInputFormat.SCAN,"");
	
		SMInputFormat format = new SMInputFormat();
		format.setConf(job.getConfiguration());  
		
		
	    SparkConf sparkConf = new SparkConf().setAppName("JavaSparkPi").setMaster("local");
	    sparkConf.set("spark.broadcast.compress", "false");
	    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		
    JavaPairRDD<RowLocation, ExecRow> table = jsc.newAPIHadoopRDD(job.getConfiguration(), SMInputFormat.class, RowLocation.class, ExecRow.class);
    
/*    
    int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
    int n = 100000 * slices;
    List<Integer> l = new ArrayList<Integer>(n);
    for (int i = 0; i < n; i++) {
      l.add(i);
    }

    
    JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

    int count = dataSet.map(new Function<Integer, Integer>() {
      @Override
      public Integer call(Integer integer) {
        double x = Math.random() * 2 - 1;
        double y = Math.random() * 2 - 1;
        return (x * x + y * y < 1) ? 1 : 0;
      }
    }).reduce(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer integer, Integer integer2) {
        return integer + integer2;
      }
    });
    System.out.println("Pi is roughly " + 4.0 * count / n);
*/
/*
    jsc.stop();
    */
  }
}