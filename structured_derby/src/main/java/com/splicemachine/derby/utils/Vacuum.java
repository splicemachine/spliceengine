package com.splicemachine.derby.utils;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.impl.TransactionId;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.*;
import java.util.List;

/**
 * Utility for Vacuuming Splice.
 *
 * @author Scott Fines
 * Date: 3/19/14
 */
public class Vacuum {

		private final Connection connection;
		private final HBaseAdmin admin;

		public Vacuum(Connection connection) throws SQLException {
				this.connection = connection;
				try {
						this.admin = new HBaseAdmin(SpliceConstants.config);
				} catch (MasterNotRunningException e) {
						throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
				} catch (ZooKeeperConnectionException e) {
						throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
				}
		}

		public void vacuumDatabase() throws SQLException{
				ensurePriorTransactionsComplete();

				//get all the conglomerates from sys.sysconglomerates
				PreparedStatement ps = null;
				ResultSet rs = null;
				LongOpenHashSet activeConglomerates = LongOpenHashSet.newInstance();
				try{
						ps = connection.prepareStatement("select conglomeratenumber from sys.sysconglomerates");

						rs = ps.executeQuery();

						while(rs.next()){
								activeConglomerates.add(rs.getLong(1));
						}
				}finally{
						if(rs!=null)
								rs.close();
						if(ps!=null)
								ps.close();
				}

				//get all the tables from HBaseAdmin
				try {
						HTableDescriptor[] hTableDescriptors = admin.listTables();

						for(HTableDescriptor table:hTableDescriptors){
								try{
										long tableConglom = Long.parseLong(Bytes.toString(table.getName()));
										if(tableConglom<1168l) continue; //ignore system tables
										if(!activeConglomerates.contains(tableConglom)){
												admin.disableTable(table.getName());
												admin.deleteTable(table.getName());
										}
								}catch(NumberFormatException nfe){
										/*
										 * This is either TEMP, TRANSACTIONS, SEQUENCES, or something
										 * that's not managed by splice. Ignore it
										 */
								}
						}
				} catch (IOException e) {
						throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
				}
		}

		/*
		 * We have to make sure that all prior transactions complete. Once that happens, we know that the worldview
		 * of all outstanding transactions is the same as ours--so if a conglomerate doesn't exist in sysconglomerates,
		 * then it's not useful anymore.
		 */
		private void ensurePriorTransactionsComplete() throws SQLException {
				EmbedConnection embedConnection = (EmbedConnection)connection;

				TransactionId txnId = new TransactionId(embedConnection.getLanguageConnection().getTransactionExecute().getActiveStateTxIdString());

				//wait for all transactions prior to us to complete, but only wait for so long
				try{
						List<TransactionId> stillActiveTransactions = waitForConcurrentTransactions(txnId);
						if(stillActiveTransactions.size()>0){
								//we can't do anything, blow up
								throw PublicAPI.wrapStandardException(
												ErrorState.DDL_ACTIVE_TRANSACTIONS.newException("VACUUM", stillActiveTransactions.toString()));
						}

				}catch(StandardException se){
						throw PublicAPI.wrapStandardException(se);
				}
		}

		private List<TransactionId> waitForConcurrentTransactions(TransactionId txnId) throws StandardException {
				List<TransactionId> active;
				TransactionManager tm = HTransactorFactory.getTransactionManager();

				long timeRemaining = SpliceConstants.ddlDrainingMaximumWait;
				long pollPeriod = SpliceConstants.pause;
				int tryNum = 1;
				try {
						do{
								active = tm.getActiveTransactionIds(txnId);
								active.remove(txnId);
								if(active.size()<=0) return active;
								long time = System.currentTimeMillis();

								try {
										Thread.sleep(Math.min(tryNum*pollPeriod,timeRemaining));
								} catch (InterruptedException e) {
										throw new IOException(e);
								}
								timeRemaining-=(System.currentTimeMillis()-time);
								tryNum++;
						}while(timeRemaining>0);
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}

				return active;
		}

		public void shutdown() throws SQLException {
				try {
						admin.close();
				} catch (IOException e) {
						throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
				}
		}
}
