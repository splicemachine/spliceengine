package com.splicemachine.derby.impl.sql.execute;

import com.google.common.io.Closeables;
import com.splicemachine.derby.impl.sql.execute.index.SpliceIndexProtocol;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
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

}
