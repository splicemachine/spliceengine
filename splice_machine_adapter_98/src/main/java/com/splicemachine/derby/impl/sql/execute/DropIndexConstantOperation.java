package com.splicemachine.derby.impl.sql.execute;

import com.google.common.io.Closeables;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.hbase.table.SpliceRpcController;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.TxnView;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
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
public class DropIndexConstantOperation extends AbstractDropIndexConstantOperation {

	public DropIndexConstantOperation(String fullIndexName, String indexName, String tableName,
                                      String schemaName, UUID tableId, long tableConglomerateId) {
				super(fullIndexName,indexName,tableName,schemaName,tableId,tableConglomerateId);
		}

	@Override
	public void dropIndexTrigger(final long tableConglomId, final long indexConglomId, final TxnView userTxn) throws StandardException{
		//drop the index trigger from the main table
		HTableInterface mainTable = SpliceAccessManager.getHTable(tableConglomId);
		try {
			mainTable.coprocessorService(SpliceMessage.SpliceIndexManagementService.class,
            HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW,new Batch.Call<SpliceMessage.SpliceIndexManagementService, Void>() {
				@Override
				public Void call(SpliceMessage.SpliceIndexManagementService instance) throws IOException {
					SpliceRpcController controller = new SpliceRpcController();
					SpliceMessage.DropIndexRequest request = SpliceMessage.DropIndexRequest.newBuilder()
                    .setIndexConglomId(indexConglomId).setBaseConglomId(tableConglomId).setTxnId(userTxn.getTxnId()).build();
					instance.dropIndex(controller,request,new BlockingRpcCallback<SpliceMessage.DropIndexResponse>());
					if(controller.failed()){
						throw Exceptions.getIOException(controller.getThrowable());
					}
					return null;
				}
			});
		} catch (Throwable throwable) {
			throw Exceptions.parseException(throwable);
		}finally{
			Closeables.closeQuietly(mainTable);
		}
	}
}
