package com.splicemachine.derby.impl.sql.execute.actions;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.google.common.io.Closeables;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;

import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.coprocessor.SpliceMessage.SpliceIndexManagementService;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.hbase.table.SpliceRpcController;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.impl.TransactionId;

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
		public DropIndexConstantOperation(String fullIndexName,String indexName,String tableName,
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
				TransactionId parent = new TransactionId(getTransactionId(tc));
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
						mainTable.coprocessorService(SpliceIndexManagementService.class,
										HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW,
										new Batch.Call<SpliceIndexManagementService, Void>() {
												@Override
												public Void call(SpliceIndexManagementService instance) throws IOException {
													
													SpliceMessage.DropIndexRequest.Builder request = SpliceMessage.DropIndexRequest.newBuilder();
													request.setBaseConglomId(tableConglomId);
													request.setIndexConglomId(indexConglomId);
													BlockingRpcCallback<SpliceMessage.DropIndexResponse> rpcCallback = new BlockingRpcCallback<SpliceMessage.DropIndexResponse>();
													SpliceRpcController controller = new SpliceRpcController();
													instance.dropIndex(controller, request.build(), rpcCallback);
													Throwable error = controller.getThrowable();
													if(error!=null) throw Exceptions.getIOException(error);
													return null;
												}
										}); ;
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
				return new TransactionId(getTransactionId(metaTxn));
		}
}
