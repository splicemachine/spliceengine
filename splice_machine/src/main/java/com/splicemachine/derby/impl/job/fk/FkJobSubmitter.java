package com.splicemachine.derby.impl.job.fk;

import com.google.common.collect.ImmutableList;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.FKTentativeDDLDesc;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.utils.DataDictionaryUtils;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobScheduler;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.writecontextfactory.FKConstraintInfo;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * Encapsulate code/error-handling necessary to submit a FkJob.  The code here was just extracted from
 * the ConstantAction where it is used because it got a bit large/complex.
 * <p/>
 * Modifies existing write context factories on remote/all nodes when we add or drop a foreign key constraint.
 */
public class FkJobSubmitter {

    private static final Logger LOG = Logger.getLogger(FkJobSubmitter.class);

    private final DataDictionary dataDictionary;
    private final SpliceTransactionManager transactionManager;
    private final ReferencedKeyConstraintDescriptor referencedConstraint;
    private final ConstraintDescriptor foreignKeyConstraintDescriptor;
    private final DDLChangeType ddlChangeType;

    public FkJobSubmitter(DataDictionary dataDictionary,
                          SpliceTransactionManager transactionManager,
                          ReferencedKeyConstraintDescriptor referencedConstraint,
                          ConstraintDescriptor foreignKeyConstraintDescriptor, DDLChangeType ddlChangeType) {
        this.dataDictionary = dataDictionary;
        this.transactionManager = transactionManager;
        this.referencedConstraint = referencedConstraint;
        this.foreignKeyConstraintDescriptor = foreignKeyConstraintDescriptor;
        this.ddlChangeType = ddlChangeType;
    }

    /**
     * Creates jobs for the parent and child conglomerates, submits them, and waits for completion of both.
     */
    public void submit() throws StandardException {

        // Format IDs for the new foreign key.
        //
        ColumnDescriptorList backingIndexColDescriptors = referencedConstraint.getColumnDescriptors();
        int backingIndexFormatIds[] = DataDictionaryUtils.getFormatIds(backingIndexColDescriptors);
        int referencedConglomerateId = (int) referencedConstraint.getIndexConglomerateDescriptor(dataDictionary).getConglomerateNumber();

        // Need the conglomerate ID of the backing index of the new FK.
        //
        List<ConstraintDescriptor> newForeignKey = ImmutableList.of(foreignKeyConstraintDescriptor);
        long backingIndexConglomerateIds = DataDictionaryUtils.getBackingIndexConglomerateIdsForForeignKeys(newForeignKey).get(0);

        String referencedTableVersion = referencedConstraint.getTableDescriptor().getVersion();
        String referencedTableName = referencedConstraint.getTableDescriptor().getName();

        HTableInterface parentHTable = SpliceAccessManager.getHTable(Long.toString(referencedConglomerateId).getBytes());
        HTableInterface childHTable = SpliceAccessManager.getHTable(Long.toString(backingIndexConglomerateIds).getBytes());

        JobInfo parentJobInfo = null;
        JobFuture parentFuture = null;
        JobInfo childJobInfo = null;
        JobFuture childFuture = null;
        try {
            long start = System.currentTimeMillis();

            FKTentativeDDLDesc descriptor = new FKTentativeDDLDesc(
                    new FKConstraintInfo((ForeignKeyConstraintDescriptor) foreignKeyConstraintDescriptor),
                    backingIndexFormatIds,
                    referencedConglomerateId,
                    referencedTableName,
                    referencedTableVersion,
                    backingIndexConglomerateIds
            );

            JobScheduler<CoprocessorJob> jobScheduler = SpliceDriver.driver().getJobScheduler();
            TxnView activeStateTxn = transactionManager.getActiveStateTxn();

            //
            // parent
            //
            FkJob parentJob = new FkJob(parentHTable, activeStateTxn, referencedConglomerateId, ddlChangeType, descriptor);
            parentFuture = jobScheduler.submit(parentJob);
            parentJobInfo = new JobInfo(parentJob.getJobId(), parentFuture.getNumTasks(), start);
            parentJobInfo.setJobFuture(parentFuture);

            //
            // child
            //
            FkJob childJob = new FkJob(childHTable, activeStateTxn, backingIndexConglomerateIds, ddlChangeType, descriptor);
            childFuture = jobScheduler.submit(childJob);
            childJobInfo = new JobInfo(childJob.getJobId(), childFuture.getNumTasks(), start);
            childJobInfo.setJobFuture(childFuture);

            try {
                parentFuture.completeAll(parentJobInfo);
                childFuture.completeAll(childJobInfo);
            } catch (CancellationException ce) {
                throw Exceptions.parseException(ce);
            } catch (Throwable t) {
                parentJobInfo.failJob();
                childJobInfo.failJob();
                throw t;
            }
        } catch (Throwable e) {
            if (parentJobInfo != null) {
                parentJobInfo.failJob();
            }
            if (childJobInfo != null) {
                childJobInfo.failJob();
            }
            LOG.error("Couldn't create FKs on existing regions", e);
            try {
                parentHTable.close();
                childHTable.close();
            } catch (IOException e1) {
                LOG.warn("Couldn't close parentHTable", e1);
            }
            throw Exceptions.parseException(e);
        } finally {
            if (parentFuture != null) {
                try {
                    parentFuture.cleanup();
                } catch (ExecutionException e) {
                    LOG.warn("Task cleanup failure", e);
                }
            }
            if (childFuture != null) {
                try {
                    childFuture.cleanup();
                } catch (ExecutionException e) {
                    LOG.warn("Task cleanup failure", e);
                }
            }
        }
    }

}
