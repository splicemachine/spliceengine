package com.splicemachine.pipeline.writecontextfactory;

import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.derby.utils.DataDictionaryUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

/**
 * Factored out of LocalWriteContextFactory, this class holds the WriteFactory instances related to foreign keys and
 * methods for creating them.
 */
class FKWriteFactoryHolder {

    /*
     * Foreign key WriteHandlers intercept writes to parent/child tables and send them to the corresponding parent/child
     * table for existence checks. Generally one WriteHandler handles all intercepts/checks for the conglomerate
     * for this context.  Child intercept is the exception, where we will have multiple WriteHandlers if the backing
     * index is shared by multiple FKs (multiple FKs on the same child column sharing the the same backing index).
     */
    private List<ForeignKeyChildInterceptWriteFactory> childInterceptWriteFactories = Lists.newArrayList();
    private ForeignKeyChildCheckWriteFactory childCheckWriteFactory;

    private ForeignKeyParentInterceptWriteFactory parentInterceptWriteFactory;
    private ForeignKeyParentCheckWriteFactory parentCheckWriteFactory;

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // build factories from minimal information.
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


    public void addForeignKeyParentCheckWriteFactory(int[] backingIndexFormatIds, String parentTableVersion) {
        /* One instance handles all FKs that reference this primary key or unique index */
        this.parentCheckWriteFactory = new ForeignKeyParentCheckWriteFactory(backingIndexFormatIds, parentTableVersion);
    }

    public void addForeignKeyParentInterceptWriteFactory(String parentTableName, List<Long> backingIndexConglomIds) {
        /* One instance handles all FKs that reference this primary key or unique index */
        if (this.parentInterceptWriteFactory == null) {
            this.parentInterceptWriteFactory = new ForeignKeyParentInterceptWriteFactory(parentTableName, backingIndexConglomIds);
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // build factories from constraints
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


    /* Add factories for intercepting writes to FK backing indexes. */
    public void buildForeignKeyInterceptWriteFactory(DataDictionary dataDictionary, ForeignKeyConstraintDescriptor fkConstraintDesc) throws StandardException {
        ReferencedKeyConstraintDescriptor referencedConstraint = fkConstraintDesc.getReferencedConstraint();
        long keyConglomerateId;
        // FK references unique constraint.
        if (referencedConstraint.getConstraintType() == DataDictionary.UNIQUE_CONSTRAINT) {
            keyConglomerateId = referencedConstraint.getIndexConglomerateDescriptor(dataDictionary).getConglomerateNumber();
        }
        // FK references primary key constraint.
        else {
            keyConglomerateId = referencedConstraint.getTableDescriptor().getHeapConglomerateId();
        }
        byte[] hbaseTableNameBytes = Bytes.toBytes(String.valueOf(keyConglomerateId));
        childInterceptWriteFactories.add(new ForeignKeyChildInterceptWriteFactory(hbaseTableNameBytes, fkConstraintDesc));
        childCheckWriteFactory = new ForeignKeyChildCheckWriteFactory(fkConstraintDesc);
    }

    /* Add factories for *checking* existence of FK referenced primary-key or unique-index rows. */
    public void buildForeignKeyCheckWriteFactory(ReferencedKeyConstraintDescriptor cDescriptor) throws StandardException {
        ConstraintDescriptorList fks = cDescriptor.getForeignKeyConstraints(ConstraintDescriptor.ENABLED);
        if (fks.isEmpty()) {
            return;
        }
        ColumnDescriptorList backingIndexColDescriptors = cDescriptor.getColumnDescriptors();
        int backingIndexFormatIds[] = DataDictionaryUtils.getFormatIds(backingIndexColDescriptors);

        String parentTableName = cDescriptor.getTableDescriptor().getName();
        String parentTableVersion = cDescriptor.getTableDescriptor().getVersion();
        addForeignKeyParentCheckWriteFactory(backingIndexFormatIds, parentTableVersion);
        List<Long> backingIndexConglomIds = DataDictionaryUtils.getBackingIndexConglomerateIdsForForeignKeys(fks);
        addForeignKeyParentInterceptWriteFactory(parentTableName, backingIndexConglomIds);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // derived convenience properties
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public boolean hasChildCheck() {
        return childCheckWriteFactory != null;
    }

    public boolean hasParentIntercept() {
        return parentInterceptWriteFactory != null;
    }

    public boolean hasParentCheck() {
        return parentCheckWriteFactory != null;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // getters/setters
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


    public List<ForeignKeyChildInterceptWriteFactory> getChildInterceptWriteFactories() {
        return childInterceptWriteFactories;
    }

    public ForeignKeyChildCheckWriteFactory getChildCheckWriteFactory() {
        return childCheckWriteFactory;
    }

    public ForeignKeyParentInterceptWriteFactory getParentInterceptWriteFactory() {
        return parentInterceptWriteFactory;
    }

    public ForeignKeyParentCheckWriteFactory getParentCheckWriteFactory() {
        return parentCheckWriteFactory;
    }
}
