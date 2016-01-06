package com.splicemachine.derby.utils;

import java.sql.SQLException;

/**
 * Utility for Vacuuming Splice.
 *
 * @author Scott Fines
 *         Date: 3/19/14
 */
public class Vacuum{

//		private final Connection connection;
//		private final HBaseAdmin admin;

//		public Vacuum(Connection connection) throws SQLException {
//				this.connection = connection;
//				try {
//						this.admin = SpliceUtilities.getAdmin();
//				} catch (Exception e) {
//						throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
//				}
//		}

    public void vacuumDatabase() throws SQLException{
        throw new UnsupportedOperationException("IMPLEMENT");
//				ensurePriorTransactionsComplete();
//
//				//get all the conglomerates from sys.sysconglomerates
//				PreparedStatement ps = null;
//				ResultSet rs = null;
//				LongOpenHashSet activeConglomerates = LongOpenHashSet.newInstance();
//				try{
//						ps = connection.prepareStatement("select conglomeratenumber from sys.sysconglomerates");
//
//						rs = ps.executeQuery();
//
//						while(rs.next()){
//								activeConglomerates.add(rs.getLong(1));
//						}
//				}finally{
//						if(rs!=null)
//								rs.close();
//						if(ps!=null)
//								ps.close();
//				}
//
//				//get all the tables from HBaseAdmin
//				try {
//						HTableDescriptor[] hTableDescriptors = admin.listTables();
//
//						for(HTableDescriptor table:hTableDescriptors){
//								try{
//										long tableConglom = Long.parseLong(Bytes.toString(table.getName()));
//										if(tableConglom<1168l) continue; //ignore system tables
//										if(!activeConglomerates.contains(tableConglom)){
//											SpliceUtilities.deleteTable(admin, table);
//										}
//								}catch(NumberFormatException nfe){
//										/*
//										 * This is either TEMP, TRANSACTIONS, SEQUENCES, or something
//										 * that's not managed by splice. Ignore it
//										 */
//								}
//						}
//				} catch (IOException e) {
//						throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
//				}
    }

		/*
         * We have to make sure that all prior transactions complete. Once that happens, we know that the worldview
		 * of all outstanding transactions is the same as ours--so if a conglomerate doesn't exist in sysconglomerates,
		 * then it's not useful anymore.
		 */
//		private void ensurePriorTransactionsComplete() throws SQLException {
//				EmbedConnection embedConnection = (EmbedConnection)connection;
//
//        TransactionController transactionExecute = embedConnection.getLanguageConnection().getTransactionExecute();
//        TxnView activeStateTxn = ((SpliceTransactionManager) transactionExecute).getActiveStateTxn();
//
//
//				//wait for all transactions prior to us to complete, but only wait for so long
//				try{
//						long activeTxn = waitForConcurrentTransactions(activeStateTxn);
//						if(activeTxn>0){
//								//we can't do anything, blow up
//								throw PublicAPI.wrapStandardException(
//												ErrorState.DDL_ACTIVE_TRANSACTIONS.newException("VACUUM", activeTxn));
//						}
//
//				}catch(StandardException se){
//						throw PublicAPI.wrapStandardException(se);
//				}
//		}

//    private long waitForConcurrentTransactions(TxnView txn) throws StandardException {
//        ActiveTransactionReader reader = new ActiveTransactionReader(0l,txn.getTxnId(),null);
//        long timeRemaining = SpliceConstants.ddlDrainingMaximumWait;
//        long pollPeriod = SpliceConstants.pause;
//        int tryNum = 1;
//        long activeTxn;
//
//        try {
//            do {
//                activeTxn = -1l;
//
//                TxnView next;
//                try (Stream<TxnView> activeTransactions = reader.getActiveTransactions()){
//                    while((next = activeTransactions.next())!=null){
//                        long txnId = next.getTxnId();
//                        if(txnId!=txn.getTxnId()){
//                            activeTxn = txnId;
//                            break;
//                        }
//                    }
//                }
//
//                if(activeTxn<0) return activeTxn; //no active transactions
//
//                long time = System.currentTimeMillis();
//
//					try {
//						Thread.sleep(Math.min(tryNum*pollPeriod,timeRemaining));
//					} catch (InterruptedException e) {
//						throw new IOException(e);
//					}
//					timeRemaining-=(System.currentTimeMillis()-time);
//					tryNum++;
//				} while (timeRemaining>0);
//			} catch (IOException | StreamException e) {
//				throw Exceptions.parseException(e);
//			}
//
//        return activeTxn;
//		} // end waitForConcurrentTransactions

//		public void shutdown() throws SQLException {
//				try {
//						admin.close();
//				} catch (IOException e) {
//						throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
//				}
//		}
}
