package com.splicemachine.mrio.api;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

public class SpliceMRUtil {
	
	HashMap<String, Connection> txnMap = new HashMap<String, Connection>();
	
	/**
	 * Start root transaction to be used in transactional executions. 
	 * @return transaction ID
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws SQLException 
	 */
	public String startReadOnlyRootTxn(String connStr) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException{
		String txnID = null;
		SQLUtil sqlUtil = SQLUtil.getInstance(connStr);
	    Connection conn = sqlUtil.createConn();
	    String pTxsID = sqlUtil.getTransactionID(conn);
	    txnID = pTxsID;
	    
	    txnMap.put(txnID, conn);
	    
		return txnID;
	}
	
	public void commitTxn(String txnID) throws Exception{
		Connection conn = txnMap.get(txnID);
		if(conn == null)
			throw new Exception("ERROR: Trying to commit a non-exist transaction");
		else{
			conn.commit();
			conn.close();
			txnMap.remove(txnID);
		}
	}
	
	public void rollbackTxn(String txnID) throws Exception{
		Connection conn = txnMap.get(txnID);
		if(conn == null)
			throw new Exception("ERROR: Trying to rollback a non-exist transaction");
		else{
			conn.rollback();
			conn.close();
			txnMap.remove(txnID);
		}
	}
}
