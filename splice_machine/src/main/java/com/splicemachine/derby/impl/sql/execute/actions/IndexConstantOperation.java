package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.KVPairFunction;
import com.splicemachine.derby.stream.index.HTableScannerBuilder;
import com.splicemachine.derby.stream.function.IndexTransformFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.index.HTableWriterBuilder;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.impl.*;
import org.apache.hadoop.hbase.client.Scan;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.derby.ddl.TentativeIndexDesc;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.job.index.CreateIndexJob;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.management.OperationInfo;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.job.JobFuture;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.uuid.Snowflake;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.exception.ErrorState;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.store.access.TransactionController;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

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

    protected Txn getIndexTransaction(TransactionController tc, Txn tentativeTransaction, long tableConglomId) throws StandardException {
        final TxnView wrapperTxn = ((SpliceTransactionManager)tc).getActiveStateTxn();

        /*
         * We have an additional waiting transaction that we use to ensure that all elements
         * which commit after the demarcation point are committed BEFORE the populate part.
         */
        byte[] tableBytes = Long.toString(tableConglomId).getBytes();
        Txn waitTxn;
        try{
            waitTxn = TransactionLifecycle.getLifecycleManager().chainTransaction(wrapperTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION,false,tableBytes,tentativeTransaction);
        }catch(IOException ioe){
            LOG.error("Could not create a wait transaction",ioe);
            throw Exceptions.parseException(ioe);
        }

        //get the absolute user transaction
        TxnView uTxn = wrapperTxn;
        TxnView n = uTxn.getParentTxnView();
        while(n.getTxnId()>=0){
            uTxn = n;
            n = n.getParentTxnView();
        }
        // Wait for past transactions to die
        long oldestActiveTxn;
        try {
            oldestActiveTxn = waitForConcurrentTransactions(waitTxn, uTxn,tableConglomId);
        } catch (IOException e) {
            LOG.error("Unexpected error while waiting for past transactions to complete", e);
            throw Exceptions.parseException(e);
        }
        if (oldestActiveTxn>=0) {
            throw ErrorState.DDL_ACTIVE_TRANSACTIONS.newException("CreateIndex("+indexName+")",oldestActiveTxn);
        }
        Txn indexTxn;
        try{
            /*
             * We need to make the indexTxn a child of the wrapper, so that we can be sure
             * that the write pipeline is able to see the conglomerate descriptor. However,
             * this makes the SI logic more complex during the populate phase.
             */
            indexTxn = TransactionLifecycle.getLifecycleManager().chainTransaction(
                    wrapperTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION, true, tableBytes,waitTxn);
        } catch (IOException e) {
            LOG.error("Couldn't commit transaction for tentative DDL operation");
            // TODO must cleanup tentative DDL change
            throw Exceptions.parseException(e);
        }
        return indexTxn;
    }

    protected void populateIndex(Activation activation,
                                 int[] baseColumnPositions,
                                 boolean[] descColumns,
                                 long tableConglomId,
                                 HTableInterface table,
                                 TransactionController txnControl,
                                 Txn indexTransaction,
                                 long demarcationPoint,
                                 TentativeIndexDesc tentativeIndexDesc) throws StandardException {
        String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
				/*
				 * Backfill the index with any existing data.
				 *
				 * It's possible that the index will be created on the same node as some system tables are located.
				 * This means that there
				 */
        //TODO -sf- replace this name with the actual SQL being issued
        Snowflake snowflake = SpliceDriver.driver().getUUIDGenerator();
        StatementInfo statementInfo = new StatementInfo(String.format("populate index on %s",tableName),userId,
                ((SpliceTransactionManager)activation.getTransactionController()).getActiveStateTxn(),1, SpliceDriver.driver().getUUIDGenerator());
        OperationInfo populateIndexOp = new OperationInfo(SpliceDriver.driver().getUUIDGenerator().nextUUID(),
                statementInfo.getStatementUuid(), "PopulateIndex", null, false,-1l);
        statementInfo.setOperationInfo(Arrays.asList(populateIndexOp));
        boolean unique = tentativeIndexDesc.isUnique();
        boolean uniqueWithDuplicateNulls = tentativeIndexDesc.isUniqueWithDuplicateNulls();
        long indexConglomId = tentativeIndexDesc.getConglomerateNumber();
        Txn childTxn = null;
        try {
            SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) txnControl).findConglomerate(tableConglomId);

            int[] formatIds = conglomerate.getFormat_ids();
            int[] columnOrdering = conglomerate.getColumnOrdering();

            DataSetProcessor dsp = StreamUtils.getDataSetProcessor();
            childTxn = beginChildTransaction(indexTransaction, indexConglomId);
            Scan tableScan = createScan();
            HTableScannerBuilder hTableScannerBuilder = new HTableScannerBuilder()
                    .transaction(indexTransaction)
                    .demarcationPoint(demarcationPoint)
                    .indexColToMainColPosMap(baseColumnPositions)
                    .scan(tableScan);
            HTableWriterBuilder builder = new HTableWriterBuilder()
                    .heapConglom(indexConglomId)
                    .txn(childTxn);

            DataSet<KVPair> dataset = dsp.getHTableScanner(hTableScannerBuilder, (new Long(tableConglomId)).toString());
            IndexTransformFunction indexTransformerFunction =
                    new IndexTransformFunction(
                            baseColumnPositions,
                            unique,
                            uniqueWithDuplicateNulls,
                            descColumns,
                            columnOrdering,
                            formatIds);
            DataSet<LocatedRow> result = dataset.map(indexTransformerFunction).index(new KVPairFunction()).writeKVPair(builder);
            childTxn.commit();
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }
    protected Txn beginChildTransaction(TxnView parentTxn, long indexConglomId) throws IOException{
        TxnLifecycleManager tc = TransactionLifecycle.getLifecycleManager();
        return tc.beginChildTransaction(parentTxn,Long.toString(indexConglomId).getBytes());
    }

    private Scan createScan () throws IOException{
        Scan scan= SpliceUtils.createScan(null);
        scan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
        scan.setStartRow(new byte[0]);
        scan.setStopRow(new byte[0]);
        scan.setCacheBlocks(false);
        return scan;
    }

    protected void createIndex(Activation activation, DDLChange ddlChange,
                               HTableInterface table, TableDescriptor td) throws StandardException {
        JobFuture future = null;
        JobInfo info = null;
        /*StatementInfo statementInfo = new StatementInfo(String.format("create index on %s",tableName),userId,
                activation.getTransactionController().getActiveStateTxIdString(),1, SpliceDriver.driver().getUUIDGenerator());*/

        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        int[] columnOrdering = null;
        int[] formatIds;
        ColumnDescriptorList cdList = td.getColumnDescriptorList();
        int numCols =  cdList.size();
        formatIds = new int[numCols];
        for (int j = 0; j < numCols; ++j) {
            ColumnDescriptor columnDescriptor = cdList.elementAt(j);
            formatIds[j] = columnDescriptor.getType().getNull().getTypeFormatId();
        }
        ConstraintDescriptorList constraintDescriptors = dd.getConstraintDescriptors(td);
        for(int i=0;i<constraintDescriptors.size();i++){
            ConstraintDescriptor cDescriptor = constraintDescriptors.elementAt(i);
            if (cDescriptor.getConstraintType() == DataDictionary.PRIMARYKEY_CONSTRAINT){
                int[] referencedColumns = cDescriptor.getReferencedColumns();
                columnOrdering = new int[referencedColumns.length];
                for (int j = 0; j < referencedColumns.length; ++j){
                    columnOrdering[j] = referencedColumns[j] - 1;
                }
            }
        }

        /*statementInfo.setOperationInfo(Arrays.asList(new OperationInfo(statementInfo.getStatementUuid(),
                SpliceDriver.driver().getUUIDGenerator().nextUUID(), "CreateIndex", null, false, -1l)));*/
        try {
            long start = System.currentTimeMillis();
            CreateIndexJob job = new CreateIndexJob(table, ddlChange, columnOrdering, formatIds);
            future = SpliceDriver.driver().getJobScheduler().submit(job);
            info = new JobInfo(job.getJobId(),future.getNumTasks(),start);
            info.setJobFuture(future);
            try{
                future.completeAll(info); //TODO -sf- add status information
            }catch(CancellationException ce){
                throw Exceptions.parseException(ce);
            }catch(Throwable t){
                info.failJob();
                throw t;
            }
            //statementInfo.completeJob(info);
        } catch (Throwable e) {
            if(info!=null) info.failJob();
            LOG.error("Couldn't create indexes on existing regions", e);
            try {
                table.close();
            } catch (IOException e1) {
                LOG.warn("Couldn't close table", e1);
            }
            throw Exceptions.parseException(e);
        }finally {
            cleanupFuture(future);
        }
    }

    private void cleanupFuture(JobFuture future) throws StandardException {
        if (future!=null) {
            try {
                future.cleanup();
            } catch (ExecutionException e) {
                LOG.error("Couldn't cleanup future", e);
                //noinspection ThrowFromFinallyBlock
                throw Exceptions.parseException(e.getCause());
            }
        }
    }

    private int[] transformColumnOrdering(ColumnOrdering[] columnOrdering) {
        int[] columnOrder = new int[columnOrdering.length];
        for(int i=0;i<columnOrdering.length;i++){
            columnOrder[i] = columnOrdering[i].getColumnId();
        }
        return columnOrder;
    }
}
