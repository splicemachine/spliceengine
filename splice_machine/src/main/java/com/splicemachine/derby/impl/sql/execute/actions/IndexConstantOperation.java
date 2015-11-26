package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.KVPairFunction;
import com.splicemachine.derby.stream.index.HTableScannerBuilder;
import com.splicemachine.derby.stream.function.IndexTransformFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.index.HTableWriterBuilder;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import org.apache.log4j.Logger;
import org.sparkproject.guava.primitives.Ints;
import java.io.IOException;

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



    protected void populateIndex(Activation activation,
                                 Txn indexTransaction,
                                 long demarcationPoint,
                                 DDLMessage.TentativeIndex tentativeIndex) throws StandardException {
        String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
				/*
				 * Backfill the index with any existing data.
				 *
				 * It's possible that the index will be created on the same node as some system tables are located.
				 */
        Txn childTxn = null;
        try {
            DataSetProcessor dsp = StreamUtils.getDataSetProcessor();
            StreamUtils.setupSparkJob(dsp, activation, this.toString(), "admin");
            childTxn = beginChildTransaction(indexTransaction, tentativeIndex.getIndex().getConglomerate());
            HTableScannerBuilder hTableScannerBuilder = new HTableScannerBuilder()
                    .transaction(indexTransaction)
                    .demarcationPoint(demarcationPoint)
                    .indexColToMainColPosMap(Ints.toArray(tentativeIndex.getIndex().getIndexColsToMainColMapList()))
                    .scan(DDLUtils.createFullScan());
            HTableWriterBuilder builder = new HTableWriterBuilder()
                    .heapConglom(tentativeIndex.getIndex().getConglomerate())
                    .txn(childTxn);
            DataSet<KVPair> dataset = dsp.getHTableScanner(hTableScannerBuilder,
					HBaseTableInfoFactory.getInstance().getTableInfo(Long.toString(tentativeIndex.getTable().getConglomerate())));
            DataSet<LocatedRow> result = dataset.map(new IndexTransformFunction(tentativeIndex))
                    .index(new KVPairFunction()).writeKVPair(builder);
            childTxn.commit();
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }
    protected Txn beginChildTransaction(TxnView parentTxn, long indexConglomId) throws IOException{
        TxnLifecycleManager tc = TransactionLifecycle.getLifecycleManager();
        return tc.beginChildTransaction(parentTxn,Long.toString(indexConglomId).getBytes());
    }

}
