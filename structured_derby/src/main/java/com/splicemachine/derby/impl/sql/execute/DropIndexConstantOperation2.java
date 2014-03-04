package com.splicemachine.derby.impl.sql.execute;

import com.google.common.io.Closeables;
import com.splicemachine.derby.impl.sql.execute.actions.IndexConstantOperation;
import com.splicemachine.derby.impl.sql.execute.index.SpliceIndexProtocol;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.impl.TransactionId;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
public class DropIndexConstantOperation2 extends IndexConstantOperation{
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
		public DropIndexConstantOperation2(String fullIndexName,String indexName,String tableName,
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

				//drop the conglomerate in a child transaction
				TransactionId metaTxnId = drop(cd, td, sd, dd, lcc);

				//create a second nested transaction
				TransactionId parent = getTransactionId(tc);
				try {
						TransactionId pipelineTxn = HTransactorFactory.getTransactionManager().beginChildTransaction(parent, true,false);
						List<TransactionId> toIgnore = Arrays.asList(parent, pipelineTxn);
						//wait to ensure that all previous transactions terminate
						waitForConcurrentTransactions(pipelineTxn,toIgnore,tableConglomerateId);
						//drop the index from the write pipeline
						dropIndex(td,cd);

						//TODO -sf- wait for all transactions to complete, then drop the
						//physical hbase table
						HTransactorFactory.getTransactionManager().commit(pipelineTxn);

				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
		}

		private void dropIndex(TableDescriptor td, ConglomerateDescriptor conglomerateDescriptor) throws StandardException {
				final long tableConglomId = td.getHeapConglomerateId();
				final long indexConglomId = conglomerateDescriptor.getConglomerateNumber();

				//drop the index trigger from the main table
				HTableInterface mainTable = SpliceAccessManager.getHTable(tableConglomId);
				try {
						mainTable.coprocessorExec(SpliceIndexProtocol.class,
										HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW,
										new Batch.Call<SpliceIndexProtocol, Void>() {
												@Override
												public Void call(SpliceIndexProtocol instance) throws IOException {
														instance.dropIndex(indexConglomId,tableConglomId);
														return null;
												}
										}) ;
				} catch (Throwable throwable) {
						throw Exceptions.parseException(throwable);
				}finally{
						Closeables.closeQuietly(mainTable);
				}
		}

		private TransactionId drop(ConglomerateDescriptor cd,
											TableDescriptor td,
											SchemaDescriptor sd,
											DataDictionary dd,
											LanguageConnectionContext lcc) throws StandardException {
				TransactionController metaTxn = lcc.getTransactionExecute().startNestedUserTransaction(false, true);

				DependencyManager dm = dd.getDependencyManager();
				dm.invalidateFor(cd,DependencyManager.DROP_INDEX,lcc);

				ConglomerateDescriptor[] congDescs = dd.getConglomerateDescriptors(cd.getConglomerateNumber());

				boolean dropConglom = false;
				ConglomerateDescriptor physicalCd;
				if(congDescs.length==1)
						dropConglom=true;
				else{
						physicalCd = cd.describeSharedConglomerate(congDescs,true);
						IndexRowGenerator othersIRG = physicalCd.getIndexDescriptor();
						boolean needNewConglomerate = (cd.getIndexDescriptor().isUnique() && !othersIRG.isUnique()) ||
										(cd.getIndexDescriptor().isUniqueWithDuplicateNulls() && !othersIRG.isUniqueWithDuplicateNulls());
						if(needNewConglomerate){
								dropConglom = true;
						}else
								physicalCd = null;
				}
				dd.dropStatisticsDescriptors(td.getUUID(),cd.getUUID(),metaTxn);
				dd.dropConglomerateDescriptor(cd,metaTxn);

				td.removeConglomerateDescriptor(cd);
				metaTxn.commit();
				return getTransactionId(metaTxn);
		}

		private TransactionId getTransactionId(TransactionController txn) {
				return ((SpliceTransaction)((SpliceTransactionManager)txn).getRawTransaction()).getTransactionId();
		}
}
