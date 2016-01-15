package com.splicemachine.derby.test.framework;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.splicemachine.utils.SpliceLogUtils;

public class SpliceSparkWatcher extends TestWatcher {
	private static final Logger LOG = Logger.getLogger(SpliceSparkWatcher.class);
	public JavaSparkContext jsc;
	public String appName;
	
	public SpliceSparkWatcher(String appName) {
		this.appName = appName;
	}
	
	@Override
	protected void starting(Description description) {
		super.starting(description);
		SpliceLogUtils.trace(LOG, "starting spark");
    	SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster("local");
    	sparkConf.set("spark.broadcast.compress", "false"); // Will attempt to use Snappy without this set.
    	sparkConf.set("spark.driver.allowMultipleContexts", "true"); // SPARK-2243
    	jsc = new JavaSparkContext(sparkConf);
	}

	@Override
	protected void finished(Description description) {
		super.finished(description);
		SpliceLogUtils.trace(LOG, "stopping spark");
		jsc.stop();
	}
}
