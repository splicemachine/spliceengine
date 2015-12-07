package com.splicemachine.derby.impl.job.fk;

import com.google.common.collect.ImmutableList;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.utils.DataDictionaryUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.TxnView;
import org.apache.log4j.Logger;
import java.util.List;

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
        int backingIndexFormatIds[] = backingIndexColDescriptors.getFormatIds();
        int referencedConglomerateId = (int) referencedConstraint.getIndexConglomerateDescriptor(dataDictionary).getConglomerateNumber();

        // Need the conglomerate ID of the backing index of the new FK.
        //
        List<ConstraintDescriptor> newForeignKey = ImmutableList.of(foreignKeyConstraintDescriptor);
        long backingIndexConglomerateIds = DataDictionaryUtils.getBackingIndexConglomerateIdsForForeignKeys(newForeignKey).get(0);

        String referencedTableVersion = referencedConstraint.getTableDescriptor().getVersion();
        String referencedTableName = referencedConstraint.getTableDescriptor().getName();
        TxnView activeStateTxn = transactionManager.getActiveStateTxn();

        DDLChange ddlChange = ProtoUtil.createTentativeFKConstaint((ForeignKeyConstraintDescriptor) foreignKeyConstraintDescriptor, activeStateTxn.getTxnId(),
                referencedConglomerateId, referencedTableName, referencedTableVersion, backingIndexFormatIds, backingIndexConglomerateIds);

        DDLUtils.notifyMetadataChangeAndWait(ddlChange);
    }

}
