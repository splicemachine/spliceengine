package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.impl.sql.execute.index.DistributedPopulateIndexJob;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import org.sparkproject.guava.primitives.Ints;
import com.splicemachine.EngineDriver;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.*;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.List;

public abstract class IndexConstantOperation extends DDLSingleTableConstantOperation {
	private static final Logger LOG = Logger.getLogger(IndexConstantOperation.class);
	public String indexName;
	public String tableName;
	public String schemaName;


    protected	IndexConstantOperation(UUID tableId){
        super(tableId);
    }

	/**
	 *	Make the ConstantAction for a CREATE/DROP INDEX statement.
	 *
	 *	@param	tableId				The table uuid
	 *	@param	indexName			Index name.
	 *	@param	tableName			The table name
	 *	@param	schemaName					Schema that index lives in.
	 *
	 */
	protected	IndexConstantOperation(UUID tableId,
								String indexName,
								String tableName,
								String schemaName) {
		super(tableId);
		this.indexName = indexName;
		this.tableName = tableName;
		this.schemaName = schemaName;
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(schemaName != null, "Schema name is null");
	}

    // CLASS METHODS

	/**
	  *	Get the index name.
	  *
	  *	@return	the name of the index
	  */
    public	String	getIndexName() { 
    	SpliceLogUtils.trace(LOG, "getIndexName %s",indexName);
    	return indexName; 
    }

	/**
	 * Set the index name at execution time.
	 * Useful for unnamed constraints which have a backing index.
	 *
	 * @param indexName		The (generated) index name.
	 */
	public void setIndexName(String indexName) {
    	SpliceLogUtils.trace(LOG, "setIndexName %s",indexName);		
		this.indexName = indexName;
	}

	@SuppressWarnings("unchecked")
	@SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE",justification = "Dead variable is a side effect of writing data")
	protected void populateIndex(Activation activation,
                                 Txn indexTransaction,
                                 long demarcationPoint,
                                 DDLMessage.TentativeIndex tentativeIndex,
                                 TableDescriptor td) throws StandardException {
        // String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
		/*
		 * Backfill the index with any existing data.
		 *
		 * It's possible that the index will be created on the same node as some system tables are located.
		 */
        Txn childTxn;
        try {
			// This preps the scan to emulate TableScanOperation and use that code path
			List<Integer> indexCols = tentativeIndex.getIndex().getIndexColsToMainColMapList();
			List<Integer> allFormatIds = tentativeIndex.getTable().getFormatIdsList();
			List<Integer> columnOrdering = tentativeIndex.getTable().getColumnOrderingList();
			int[] rowDecodingMap = new int[allFormatIds.size()];
			int[] baseColumnMap = new int[allFormatIds.size()];
			int counter = 0;
			int[] indexFormatIds = new int[indexCols.size()];
			for (int i = 0; i < rowDecodingMap.length;i++) {
				rowDecodingMap[i] = -1;
				if (indexCols.contains(i+1)) {
					baseColumnMap[i] = counter;
					indexFormatIds[counter] = allFormatIds.get(indexCols.get(indexCols.indexOf(i+1))-1);
					counter++;
				} else {
					baseColumnMap[i] = -1;
				}
			}
			// Determine which keys it scans...
			FormatableBitSet accessedKeyCols = new FormatableBitSet(columnOrdering.size());
			for (int i = 0; i < columnOrdering.size(); i++) {
				if (indexCols.contains(columnOrdering.get(i)+1))
					accessedKeyCols.set(i);
			}
			for (int i = 0; i < baseColumnMap.length;i++) {
				if (columnOrdering.contains(i))
					rowDecodingMap[i] = -1;
				else
					rowDecodingMap[i] = baseColumnMap[i];
			}

            DistributedDataSetProcessor dsp =EngineDriver.driver().processorFactory().distributedProcessor();

            childTxn = beginChildTransaction(indexTransaction, tentativeIndex.getIndex().getConglomerate());
			ScanSetBuilder<LocatedRow> builder = dsp.newScanSet(null,Long.toString(tentativeIndex.getTable().getConglomerate()));
			builder.tableDisplayName(tableName)
			.demarcationPoint(demarcationPoint)
			.transaction(indexTransaction)
			.scan(DDLUtils.createFullScan())
			.keyColumnEncodingOrder(Ints.toArray(tentativeIndex.getTable().getColumnOrderingList()))
			.execRowTypeFormatIds(indexFormatIds)
			.reuseRowLocation(false)
			.operationContext(dsp.createOperationContext(activation))
			.rowDecodingMap(rowDecodingMap)
			.keyColumnTypes(ScanOperation.getKeyFormatIds(
					Ints.toArray(tentativeIndex.getTable().getColumnOrderingList()),
					Ints.toArray(tentativeIndex.getTable().getFormatIdsList())
					))
			.keyDecodingMap(ScanOperation.getKeyDecodingMap(accessedKeyCols,
					Ints.toArray(tentativeIndex.getTable().getColumnOrderingList()),
					baseColumnMap
					))
			.accessedKeyColumns(accessedKeyCols)
			.template(WriteReadUtils.getExecRowFromTypeFormatIds(indexFormatIds));
			String scope = this.getScopeName();
			String prefix = StreamUtils.getScopeString(this);
			String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
			String jobGroup = userId + " <" +indexTransaction.getTxnId() +">";
			EngineDriver.driver().getOlapClient().execute(new DistributedPopulateIndexJob(childTxn, builder, scope, jobGroup, prefix, tentativeIndex, indexFormatIds));
            childTxn.commit();
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        } catch (TimeoutException e) {
			throw Exceptions.parseException(new IOException("PopulateIndex job failed", e));
		}
	}

    protected Txn beginChildTransaction(TxnView parentTxn, long indexConglomId) throws IOException{
        TxnLifecycleManager tc = SIDriver.driver().lifecycleManager();
        return tc.beginChildTransaction(parentTxn,Bytes.toBytes(Long.toString(indexConglomId)));
    }

	public String getScopeName() {
		return String.format("%s %s", super.getScopeName(), indexName);
	}

}
