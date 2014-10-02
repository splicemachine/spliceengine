/**
 * SpliceJob which controls submission of MapReduce Job
 * - Notice: You have to call commit() after SpliceJob finished successfully,
 *           You have to call rollback() after SpliceJob failed.
 *
 * @author Yanan Jian
 * Created on: 08/14/14
 */

package com.splicemachine.mrio.api;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.Job.JobState;

public class SpliceJob extends Job{

	 private static SQLUtil sqlUtil = null;
	 private static Connection conn = null;
	 private Configuration conf = null;
	 public SpliceJob() throws IOException {
		super();
	}
	
	 public SpliceJob(Configuration conf) throws IOException {
	    super(conf, null);
	    this.conf = conf;
	  }

	 public SpliceJob(Configuration conf, String jobName) throws IOException {
	    super(conf,jobName);
	    this.conf = conf;
	  }
	 
	 /**
	  * Do not override this function! 
	  * submit() creates one transaction for all mappers
	  * Thus data read from Splice is consistent.
	  */
	 @Override
	 public void submit() throws IOException, InterruptedException, ClassNotFoundException 
	 {	
		 if(sqlUtil == null)
			 sqlUtil = SQLUtil.getInstance(super.getConfiguration().get(SpliceMRConstants.SPLICE_JDBC_STR));
			 //sqlUtil = SQLUtil.getInstance("jdbc:derby://localhost:1527/splicedb;user=splice;password=admin");
		 if (conn == null)
			try {
				conn = sqlUtil.createConn();
				sqlUtil.disableAutoCommit(conn);
				String parentTxsID = sqlUtil.getTransactionID(conn);
				System.out.println("PARENT txnID: "+parentTxsID);
				//Thread.sleep(40000000);
				super.getConfiguration().set(SpliceMRConstants.SPLICE_TRANSACTION_ID, parentTxsID);
					
			} catch (SQLException e1) {
					throw new IOException(e1);
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		 System.out.println("Submitting job...");
		 super.submit();
		 System.out.println("Submitted job...");
	 }
	  
	  @Override
	  public boolean waitForCompletion(boolean verbose
              ) throws IOException, InterruptedException,
                       ClassNotFoundException {
		  return super.waitForCompletion(verbose);
	  }
	  
	  /**
	   * Only when you call commit() can the transaction commit
	   * Every job starts as one transaction.
	   * It is important to call commit() explicitly in client code if job succeed.
	   * @throws SQLException
	   */
	  public void commit() throws SQLException
	  {
		  sqlUtil.commit(conn);
		  sqlUtil.closeConn(conn);
		  sqlUtil.close();
	  }
	  
	  /**
	   * Only when you call rollback() can the transaction rollback
	   * Every job starts as one transaction.
	   * It is important to call rollback() explicitly in client code if job failed.
	   * @throws SQLException
	   */
	  public void rollback() throws SQLException
	  {
		  sqlUtil.rollback(conn);
		  sqlUtil.closeConn(conn);
		  sqlUtil.close();
	  }

}
