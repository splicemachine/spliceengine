package com.splicemachine.derby.impl.sql.execute;

import com.google.common.io.Closeables;
import com.splicemachine.derby.ddl.DDLChange;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.DropIndexDDLDesc;
import com.splicemachine.derby.impl.sql.execute.actions.IndexConstantOperation;
import com.splicemachine.derby.impl.sql.execute.index.SpliceIndexProtocol;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import java.io.IOException;

/**
 * DDL operation to drop an index. The approach is as follows:
 *
 * 1. Drop index from metadata
 * 2. Wait for all write operations (which modify that table) to complete
 * 3. Drop the write handler from the write pipeline
 * 4. Wait for all operations to complete
 * 5. Delete the conglomerate
 *
 * @author Scott Fines
 * Date: 3/4/14
 */
public class DropIndexConstantOperation extends IndexConstantOperation{
		private String				fullIndexName;
		private long				tableConglomerateId;
		/**
		 *	Make the ConstantAction for a DROP INDEX statement.
		 *
		 *
		 *	@param	fullIndexName		Fully qualified index name
		 *	@param	indexName			Index name.
		 *	@param	tableName			The table name
		 *	@param	schemaName			Schema that index lives in.
		 *  @param  tableId				UUID for table
		 *  @param  tableConglomerateId	heap Conglomerate Id for table
		 *
		 */
		public DropIndexConstantOperation(String fullIndexName, String indexName, String tableName,
                                      String schemaName, UUID tableId, long tableConglomerateId) {
				super(tableId, indexName, tableName, schemaName);
				this.fullIndexName = fullIndexName;
				this.tableConglomerateId = tableConglomerateId;
		}

		public	String	toString() {
				return "DROP INDEX " + fullIndexName;
		}

		@Override
		public void executeConstantAction(Activation activation) throws StandardException {
				LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
				DataDictionary dd = lcc.getDataDictionary();
				TransactionController tc = lcc.getTransactionExecute();

				dd.startWriting(lcc);

				TableDescriptor td = dd.getTableDescriptor(tableId);
				if(td==null)
						throw ErrorState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION.newException(tableName);
				if(tableConglomerateId == 0)
						tableConglomerateId = td.getHeapConglomerateId();

				SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName,tc,true);

				ConglomerateDescriptor cd = dd.getConglomerateDescriptor(indexName,sd,true);
				if(cd==null)
						throw ErrorState.LANG_INDEX_NOT_FOUND_DURING_EXECUTION.newException(fullIndexName);

        /*
         * We cannot remove the index from the write pipeline until AFTER THE USER
         * TRANSACTION completes, because ANY transaction which begins before the USER TRANSACTION
         * commits must continue updating the index as if nothing is happening (in case the user
         * aborts()). Therefore, the demarcation point for this DDL operation
         * is the USER transaction. Thus, we have the following approach:
         *
         * 1. create a child transaction
         * 2. drop the conglomerate within that child transaction
         * 3. commit the child transaction (or abort() if a problem occurs)
         * 4. Submit the USER transaction information to the write pipeline to create
         * a write pipeline filter. This filter will allow transactions which begin
         * before the USER transaction commits to continue writing to the index, while
         * transactions which occur after the commit will not.
         *
         */
				//drop the conglomerate in a child transaction
				drop(cd, td, dd, lcc);
        dropIndex(td,cd,(SpliceTransactionManager)lcc.getTransactionExecute());
		}

		private void dropIndex(TableDescriptor td, ConglomerateDescriptor conglomerateDescriptor,
                           SpliceTransactionManager userTxnManager) throws StandardException {
				final long tableConglomId = td.getHeapConglomerateId();
				final long indexConglomId = conglomerateDescriptor.getConglomerateNumber();
        TxnView uTxn = userTxnManager.getRawTransaction().getActiveStateTxn();
        //get the top-most transaction, that's the actual user transaction
        TxnView t = uTxn;
        while(t.getTxnId()!= Txn.ROOT_TRANSACTION.getTxnId()){
            uTxn = t;
            t = uTxn.getParentTxnView();
        }

        final TxnView userTxn = uTxn;
        //notify the DDLChange
        DDLChange change = new DDLChange(userTxn, DDLChangeType.DROP_INDEX);
        change.setTentativeDDLDesc(new DropIndexDDLDesc(indexConglomId,tableConglomId));
        notifyMetadataChangeAndWait(change);

				//drop the index trigger from the main table
				HTableInterface mainTable = SpliceAccessManager.getHTable(tableConglomId);
				try {
						mainTable.coprocessorExec(SpliceIndexProtocol.class,
                    HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
                    new Batch.Call<SpliceIndexProtocol, Void>() {
                        @Override
                        public Void call(SpliceIndexProtocol instance) throws IOException {
                            instance.dropIndex(indexConglomId, tableConglomId, userTxn.getTxnId());
                            return null;
                        }
                    }) ;
				} catch (Throwable throwable) {
						throw Exceptions.parseException(throwable);
				}finally{
						Closeables.closeQuietly(mainTable);
				}
		}

    private void drop(ConglomerateDescriptor cd,
                      TableDescriptor td,
                      DataDictionary dd,
                      LanguageConnectionContext lcc) throws StandardException {
        /*
         * Manage the metadata changes necessary to drop a table. Will execute
         * within a child transaction, and will commit that child transaction when completed.
         * If a failure for any reason occurs, this will rollback the child transaction,
         * then throw an exception
         */
        SpliceTransactionManager userTxnManager = (SpliceTransactionManager)lcc.getTransactionExecute();
        DependencyManager dm = dd.getDependencyManager();
        dm.invalidateFor(cd, DependencyManager.DROP_INDEX, lcc);

        dd.dropStatisticsDescriptors(td.getUUID(), cd.getUUID(), userTxnManager);
        dd.dropConglomerateDescriptor(cd,userTxnManager);

        td.removeConglomerateDescriptor(cd);
    }
}
