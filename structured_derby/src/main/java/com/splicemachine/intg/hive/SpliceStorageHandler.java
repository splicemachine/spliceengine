package com.splicemachine.intg.hive;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.PostExecute;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import com.splicemachine.mrio.api.SQLUtil;
import com.splicemachine.mrio.api.SpliceMRConstants;

public class SpliceStorageHandler extends DefaultStorageHandler
implements HiveMetaHook, HiveStoragePredicateHandler{

	private Configuration spliceConf;
	final static public String DEFAULT_PREFIX = "default.";
	private static SQLUtil sqlUtil = null;

	private static String parentTxnId = null;
	private static Connection parentConn = null;
	private static Logger Log = Logger.getLogger(SpliceStorageHandler.class.getName());
	
	public void configureTableJobProperties(TableDesc tableDesc,
		    								Map<String, String> jobProperties, boolean isInputJob) throws Exception
	{
		Properties tableProperties = tableDesc.getProperties();
		String tableName = null;
		String connStr = tableProperties.getProperty(SpliceSerDe.SPLICE_JDBC_STR);
		if(connStr == null)
			throw new Exception("Error: wrong param. Did you mean '"+SpliceSerDe.SPLICE_JDBC_STR+"'?");
		if(sqlUtil == null)
			sqlUtil = SQLUtil.getInstance(connStr);
		if(isInputJob){
			tableName = tableProperties.getProperty(SpliceSerDe.SPLICE_INPUT_TABLE_NAME);
			if(tableName == null)
				throw new Exception("Error: wrong param. Did you mean '"+SpliceSerDe.SPLICE_INPUT_TABLE_NAME+"'?");
		}
		else{
			tableName = tableProperties.getProperty(SpliceSerDe.SPLICE_OUTPUT_TABLE_NAME);
			if(tableName == null)
				throw new Exception("Error: wrong param. Did you mean '"+SpliceSerDe.SPLICE_OUTPUT_TABLE_NAME+"'?");
		}
	   
	    tableName = tableName.trim();
	    
	    if(parentConn == null){
	    	parentTxnId = startWriteJobParentTxn(connStr, tableName);   	
	    }
	   
    	jobProperties.put(SpliceSerDe.SPLICE_TRANSACTION_ID, parentTxnId);
	    if(isInputJob)
	    	jobProperties.put(SpliceSerDe.SPLICE_INPUT_TABLE_NAME, tableName);
	    else
	    	jobProperties.put(SpliceSerDe.SPLICE_OUTPUT_TABLE_NAME, tableName);
	    jobProperties.put(SpliceSerDe.SPLICE_JDBC_STR, connStr);
		
	}
	
	public String startWriteJobParentTxn(String connStr, String tableName){
		
		if (sqlUtil == null)
			sqlUtil = SQLUtil.getInstance(connStr);
		try {
			parentConn = sqlUtil.createConn();
			sqlUtil.disableAutoCommit(parentConn);
			String txsId = sqlUtil.getTransactionID(parentConn);
			PreparedStatement ps = parentConn
					.prepareStatement("call SYSCS_UTIL.SYSCS_ELEVATE_TRANSACTION(?)");
			ps.setString(1, tableName);
			ps.executeUpdate();
			return txsId;
		} catch (SQLException e) {
			return null;
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			return null;
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			return null;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			return null;
		}
		
	}
	
	@Override
	  public void configureInputJobProperties(
	    TableDesc tableDesc,
	    Map<String, String> jobProperties){
		
	    try {
			configureTableJobProperties(tableDesc, jobProperties, true);
		} catch (Exception e) {
			
			Log.error(e);
			System.exit(1);
		}
	  }
	
	@Override
	  public void configureOutputJobProperties(
	    TableDesc tableDesc,
	    Map<String, String> jobProperties) {
	    try {
			configureTableJobProperties(tableDesc, jobProperties, false);
		} catch (Exception e) {
			
			Log.error(e);
			System.exit(1);
		}
	  }
	
	@Override
	public DecomposedPredicate decomposePredicate(JobConf jobConf,
			Deserializer deserializer, ExprNodeDesc predicate) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void commitCreateTable(Table arg0) throws MetaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commitDropTable(Table arg0, boolean arg1) throws MetaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void preCreateTable(Table tbl) throws MetaException {
		
		boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
		if(isExternal){
			Log.info("Creating External table for Splice...");
		}
		
		String inputTableName = tbl.getParameters().get(SpliceSerDe.SPLICE_INPUT_TABLE_NAME);
		String outputTableName = tbl.getParameters().get(SpliceSerDe.SPLICE_OUTPUT_TABLE_NAME);
		if(inputTableName == null && outputTableName == null)
			throw new MetaException("Wrong param, did you mean " + 
									SpliceSerDe.SPLICE_INPUT_TABLE_NAME+
									" or "+ SpliceSerDe.SPLICE_OUTPUT_TABLE_NAME+" ? ");
		
		// We can choose to support user define column mapping.
		// But currently I don't think it is necessary
		// We map all columns from Splice Table to Hive Table.
		String connStr = tbl.getParameters().get(SpliceSerDe.SPLICE_JDBC_STR);
		if(connStr == null)
			throw new MetaException("Wrong param, did you mean " + 
					SpliceSerDe.SPLICE_JDBC_STR+" ? ");
		 if(sqlUtil == null)
		    	sqlUtil = SQLUtil.getInstance(connStr);
		if(inputTableName != null){
			inputTableName = inputTableName.trim();
			checkTableExists(inputTableName);
		}
		if(outputTableName != null){
			outputTableName = outputTableName.trim();
			checkTableExists(outputTableName);
		}
	}

	@Override
	public Configuration getConf()
	{
		return spliceConf;
	}
	@Override
	public void setConf(Configuration conf)
	{
		spliceConf = HBaseConfiguration.create(conf);
	}
	
	@Override
	public void preDropTable(Table arg0) throws MetaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rollbackCreateTable(Table arg0) throws MetaException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rollbackDropTable(Table arg0) throws MetaException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public Class<? extends SerDe> getSerDeClass() {
	    return SpliceSerDe.class;
	}
	
	@Override
	public HiveMetaHook getMetaHook() {
	    return this;
	}
	
	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
	    return HiveSpliceTableInputFormat.class;
	}

	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {
		
	    return HiveSpliceTableOutputFormat.class;
	}

	public static void commitParentTxn() throws SQLException{
		
		if(parentConn != null){
			parentConn.commit();
			parentConn.close();
			parentConn = null;
			Log.info("transaction" + parentTxnId +"committed");
		}
			
	}

	public static void rollbackParentTxn() throws SQLException{
		
		if(parentConn != null){
			parentConn.rollback();
			parentConn.close();
			parentConn = null;
			Log.info("transaction" + parentTxnId +"rolledback");
		}
			
	}
	
	private void checkTableExists(String tableName) throws MetaException{
		try {
			if(!sqlUtil.checkTableExists(tableName))
				throw new MetaException("Error in checkTableExists, "
						+ "check Table Exists in Splice. "
						+ "Now we only support creating External Table."
						+ "Please create table in Splice first."
						);
		} catch (SQLException e) {
			throw new MetaException("Error in checkTableExists, "
									+ "check Table Exists in Splice. "
									+ e);
		}
	}
}

