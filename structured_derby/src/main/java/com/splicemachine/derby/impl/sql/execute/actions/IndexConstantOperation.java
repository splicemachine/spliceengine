package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.ddl.DDLChange;
import com.splicemachine.derby.ddl.TentativeIndexDesc;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.job.index.CreateIndexJob;
import com.splicemachine.derby.impl.job.index.PopulateIndexJob;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.management.OperationInfo;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.JobFuture;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.impl.TransactionId;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.*;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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

    protected TransactionId getIndexTransaction(TransactionController parent, TransactionController tc, TransactionId tentativeTransaction, TransactionManager transactor, long tableConglomId) throws StandardException {
        final TransactionId parentTransactionId =  new TransactionId(getTransactionId(parent));
        final TransactionId wrapperTransactionId =  new TransactionId(getTransactionId(tc));
        TransactionId indexTransaction;
        try {
            indexTransaction = transactor.beginChildTransaction(wrapperTransactionId, true, true, false, false, true, tentativeTransaction);
        } catch (IOException e) {
            LOG.error("Couldn't commit transaction for tentative DDL operation");
            // TODO must cleanup tentative DDL change
            throw Exceptions.parseException(e);
        }

        // Wait for past transactions to die
        List<TransactionId> active;
        List<TransactionId> toIgnore = Arrays.asList(parentTransactionId, wrapperTransactionId, indexTransaction);
        try {
            active = waitForConcurrentTransactions(indexTransaction, toIgnore,tableConglomId);
        } catch (IOException e) {
            LOG.error("Unexpected error while waiting for past transactions to complete", e);
            throw Exceptions.parseException(e);
        }
        if (!active.isEmpty()) {
            throw StandardException.newException(SQLState.LANG_SERIALIZABLE,
                    new RuntimeException(String.format("There are active transactions %s", active)));
        }
        return indexTransaction;
    }

    protected void populateIndex(Activation activation, int[] baseColumnPositions, boolean[] descColumns,
                                 long tableConglomId, HTableInterface table, TransactionId indexTransaction,
                                 TentativeIndexDesc tentativeIndexDesc) throws StandardException {
        String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
				/*
				 * Backfill the index with any existing data.
				 *
				 * It's possible that the index will be created on the same node as some system tables are located.
				 * This means that there
				 */
        //TODO -sf- replace this name with the actual SQL being issued
        StatementInfo statementInfo = new StatementInfo(String.format("populate index on %s",tableName),userId,
                activation.getTransactionController().getActiveStateTxIdString(),1, SpliceDriver.driver().getUUIDGenerator());
        OperationInfo populateIndexOp = new OperationInfo(statementInfo.getStatementUuid(),
                SpliceDriver.driver().getUUIDGenerator().nextUUID(), "PopulateIndex", false,-1l);
        statementInfo.setOperationInfo(Arrays.asList(populateIndexOp));
        SpliceDriver.driver().getStatementManager().addStatementInfo(statementInfo);
        JobFuture future = null;
        boolean unique = tentativeIndexDesc.isUnique();
        boolean uniqueWithDuplicateNulls = tentativeIndexDesc.isUniqueWithDuplicateNulls();
        long conglomId = tentativeIndexDesc.getConglomerateNumber();
        try{
            SpliceConglomerate conglomerate = (SpliceConglomerate)((SpliceTransactionManager)activation.getTransactionController()).findConglomerate(tableConglomId);
            PopulateIndexJob job = new PopulateIndexJob(table, indexTransaction.getTransactionIdString(),
                    conglomId, tableConglomId, baseColumnPositions, unique, uniqueWithDuplicateNulls, descColumns,
                    statementInfo.getStatementUuid(),populateIndexOp.getOperationUuid(),
                    activation.getLanguageConnectionContext().getXplainSchema(), conglomerate.getColumnOrdering(), conglomerate.getFormat_ids());

            long start = System.currentTimeMillis();
            future = SpliceDriver.driver().getJobScheduler().submit(job);
            JobInfo info = new JobInfo(job.getJobId(),future.getNumTasks(),start);
            info.setJobFuture(future);
            statementInfo.addRunningJob(populateIndexOp.getOperationUuid(),info);
            try{
                future.completeAll(info); //TODO -sf- add status information
            }catch(ExecutionException e){
                info.failJob();
                throw e;
            }catch(CancellationException ce){
                throw Exceptions.parseException(ce);
            }
            statementInfo.completeJob(info);
        } catch (ExecutionException e) {
            throw Exceptions.parseException(e.getCause());
        } catch (InterruptedException e) {
            throw Exceptions.parseException(e);
        } finally {
            String xplainSchema = activation.getLanguageConnectionContext().getXplainSchema();
            boolean explain = xplainSchema !=null &&
                    activation.getLanguageConnectionContext().getRunTimeStatisticsMode();
            SpliceDriver.driver().getStatementManager().completedStatement(statementInfo,explain? xplainSchema: null);
            cleanupFuture(future);
        }
    }

    protected void createIndex(Activation activation, DDLChange ddlChange,
                               HTableInterface table, TableDescriptor td) throws StandardException {
        String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
        JobFuture future = null;
        JobInfo info = null;
        StatementInfo statementInfo = new StatementInfo(String.format("create index on %s",tableName),userId,
                activation.getTransactionController().getActiveStateTxIdString(),1, SpliceDriver.driver().getUUIDGenerator());

        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        int[] columnOrdering = null;
        int[] formatIds= null;
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

        statementInfo.setOperationInfo(Arrays.asList(new OperationInfo(statementInfo.getStatementUuid(),
                SpliceDriver.driver().getUUIDGenerator().nextUUID(), "CreateIndex", false, -1l)));
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
            statementInfo.completeJob(info);
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
            String xplainSchema = activation.getLanguageConnectionContext().getXplainSchema();
            boolean explain = xplainSchema !=null &&
                    activation.getLanguageConnectionContext().getRunTimeStatisticsMode();
            SpliceDriver.driver().getStatementManager().completedStatement(statementInfo,explain? xplainSchema:null);
            cleanupFuture(future);
        }
    }

    protected String getTransactionId(TransactionController tc) {
        Transaction td = ((SpliceTransactionManager)tc).getRawTransaction();
        return SpliceUtils.getTransID(td);
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
}
