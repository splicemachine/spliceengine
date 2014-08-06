package com.splicemachine.mrio.api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class SpliceJob extends Job{

	 private static SQLUtil sqlUtil = SQLUtil.getInstance();
	public SpliceJob() throws IOException {
		super();
		// TODO Auto-generated constructor stub
	}
	
	public SpliceJob(Configuration conf) throws IOException {
	    super(conf, null);
	  }

	  public SpliceJob(Configuration conf, String jobName) throws IOException {
	    super(conf,jobName);
	  }
	  
	  public void setJobName(String name) throws IllegalStateException {
		    super.setJobName(name);
		    String transactionID = sqlUtil.getTransactionID();
		    
		    super.getConfiguration().set(SpliceConstants.SPLICE_TRANSACTION_ID, transactionID);
		  }
		  


}
