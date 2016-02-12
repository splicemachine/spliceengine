package com.splicemachine.derby.impl.sql.execute.actions;

import com.google.common.primitives.Ints;
import com.splicemachine.EngineDriver;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.KVPairFunction;
import com.splicemachine.derby.stream.function.IndexTransformFunction;
import com.splicemachine.derby.stream.iapi.*;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.kvpair.KVPair;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
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
                                 DDLMessage.TentativeIndex tentativeIndex,
                                 TableDescriptor td) throws StandardException {
        String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
		/*
		 * Backfill the index with any existing data.
		 *
		 * It's possible that the index will be created on the same node as some system tables are located.
		 */
        Txn childTxn;
        try {
            DistributedDataSetProcessor dsp =EngineDriver.driver().processorFactory().distributedProcessor();
            dsp.setup(activation,this.toString(),"admin"); // this replaces StreamUtils.setupSparkJob
            childTxn = beginChildTransaction(indexTransaction, tentativeIndex.getIndex().getConglomerate());
            IndexScanSetBuilder<KVPair> indexBuilder = dsp.newIndexScanSet(null,Long.toString(tentativeIndex.getTable().getConglomerate()));
            DataSet<KVPair> dataSet = indexBuilder
				.indexColToMainColPosMap(Ints.toArray(tentativeIndex.getIndex().getIndexColsToMainColMapList()))
				.transaction(indexTransaction)
				.demarcationPoint(demarcationPoint)
				.scan(DDLUtils.createFullScan())
				.buildDataSet();
			String scope = this.getScopeName();
            PairDataSet dsToWrite = dataSet
				.map(new IndexTransformFunction(tentativeIndex), null, false, true, scope + ": Prepare Index")
				.index(new KVPairFunction(), false, true, scope + ": Populate Index");
			DataSetWriter writer = dsToWrite.directWriteData()
				.destConglomerate(tentativeIndex.getIndex().getConglomerate())
				.txn(childTxn)
				.build();
            DataSet<LocatedRow> result = writer.write(); // TODO (wjkmerge): do something with result?
            childTxn.commit();
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }
    protected Txn beginChildTransaction(TxnView parentTxn, long indexConglomId) throws IOException{
        TxnLifecycleManager tc = SIDriver.driver().lifecycleManager();
        return tc.beginChildTransaction(parentTxn,Long.toString(indexConglomId).getBytes());
    }

	public String getScopeName() {
		return String.format("%s %s", super.getScopeName(), indexName);
	}

}
