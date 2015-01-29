package com.splicemachine.derby.impl.job.fk;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.job.JobFuture;
import com.splicemachine.pipeline.exception.Exceptions;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * Encapsulate code/error-handling necessary to submit a CreateFkJob.
 */
public class CreateFkJobSubmitter {

    private static final Logger LOG = Logger.getLogger(CreateFkJobSubmitter.class);

    private DataDictionary dataDictionary;
    private SpliceTransactionManager transactionManager;
    private ReferencedKeyConstraintDescriptor referencedConstraint;

    public CreateFkJobSubmitter(DataDictionary dataDictionary, SpliceTransactionManager transactionManager, ReferencedKeyConstraintDescriptor referencedConstraint) {
        this.dataDictionary = dataDictionary;
        this.transactionManager = transactionManager;
        this.referencedConstraint = referencedConstraint;
    }

    public void submit() throws StandardException {
        ColumnDescriptorList backingIndexColDescriptors = referencedConstraint.getColumnDescriptors();
        int backingIndexFormatIds[] = new int[backingIndexColDescriptors.size()];
        int col = 0;
        for (ColumnDescriptor columnDescriptor : backingIndexColDescriptors) {
            backingIndexFormatIds[col++] = columnDescriptor.getType().getNull().getTypeFormatId();
        }
        int referencedConglomerateId = (int) referencedConstraint.getIndexConglomerateDescriptor(dataDictionary).getConglomerateNumber();
        HTableInterface table = SpliceAccessManager.getHTable(Long.toString(referencedConglomerateId).getBytes());
        JobInfo info = null;
        JobFuture future = null;
        try {
            long start = System.currentTimeMillis();
            CreateFkJob job = new CreateFkJob(table, transactionManager.getActiveStateTxn(), referencedConglomerateId, backingIndexFormatIds);
            future = SpliceDriver.driver().getJobScheduler().submit(job);
            info = new JobInfo(job.getJobId(), future.getNumTasks(), start);
            info.setJobFuture(future);
            try {
                future.completeAll(info);
            } catch (CancellationException ce) {
                throw Exceptions.parseException(ce);
            } catch (Throwable t) {
                info.failJob();
                throw t;
            }
        } catch (Throwable e) {
            if (info != null) {
                info.failJob();
            }
            LOG.error("Couldn't create FKs on existing regions", e);
            try {
                table.close();
            } catch (IOException e1) {
                LOG.warn("Couldn't close table", e1);
            }
            throw Exceptions.parseException(e);
        } finally {
            if (future != null) {
                try {
                    future.cleanup();
                } catch (ExecutionException e) {
                    LOG.warn("Task cleanup failure", e);
                }
            }
        }
    }

}
