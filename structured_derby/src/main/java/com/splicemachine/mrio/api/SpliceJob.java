package com.splicemachine.mrio.api;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class SpliceJob extends Job{

	 private static SQLUtil sqlUtil = SQLUtil.getInstance();
	 private static Connection conn = null;
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
		    
		    if (conn == null)
				try {
					conn = sqlUtil.createConn();
					System.out.println("created conn");
					sqlUtil.disableAutoCommit(conn);
					String parentTxsID = sqlUtil.getTransactionID(conn);
					super.getConfiguration().set(SpliceConstants.SPLICE_TRANSACTION_ID, parentTxsID);
					System.out.println("SpliceJob, created parent TXSID:"+parentTxsID);
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		    
	 }
	  
	  @Override
	  public boolean waitForCompletion(boolean verbose
              ) throws IOException, InterruptedException,
                       ClassNotFoundException {
		  boolean isSucceed = super.waitForCompletion(verbose);
		  try {
			  if(isSucceed)
				  sqlUtil.commit(conn);
			  else
				  sqlUtil.rollback(conn);
			} catch (SQLException e) {
						// TODO Auto-generated catch block
					e.printStackTrace();
			}
				
			
		  return isSucceed;
	  }
		  


}
