package com.splicemachine.pipeline.writecontextfactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.derby.ddl.AddForeignKeyDDLDescriptor;
import com.splicemachine.derby.utils.DataDictionaryUtils;
import com.splicemachine.pipeline.ddl.DDLChange;

import java.util.List;
import java.util.Map;

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
    private Map<Long, ForeignKeyChildInterceptWriteFactory> childInterceptWriteFactories = Maps.newHashMap();
    private ForeignKeyChildCheckWriteFactory childCheckWriteFactory;

    private ForeignKeyParentInterceptWriteFactory parentInterceptWriteFactory;
    private ForeignKeyParentCheckWriteFactory parentCheckWriteFactory;

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // build factories from minimal information (used tasks/jobs to ALTER existing write contexts)
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public void addParentCheckWriteFactory(int[] backingIndexFormatIds, String parentTableVersion) {
        /* One instance handles all FKs that reference this primary key or unique index */
        this.parentCheckWriteFactory = new ForeignKeyParentCheckWriteFactory(backingIndexFormatIds, parentTableVersion);
    }

    public void addParentInterceptWriteFactory(String parentTableName, List<Long> backingIndexConglomIds) {
        /* One instance handles all FKs that reference this primary key or unique index */
        if (this.parentInterceptWriteFactory == null) {
            this.parentInterceptWriteFactory = new ForeignKeyParentInterceptWriteFactory(parentTableName, backingIndexConglomIds);
        }
    }

    public void addChildIntercept(long referencedConglomerateNumber, FKConstraintInfo fkConstraintInfo) {
        childInterceptWriteFactories.put(referencedConglomerateNumber, new ForeignKeyChildInterceptWriteFactory(referencedConglomerateNumber, fkConstraintInfo));
    }

    public void addChildCheck(FKConstraintInfo fkConstraintInfo) {
        childCheckWriteFactory = new ForeignKeyChildCheckWriteFactory(fkConstraintInfo);
    }

    /**
     * Convenience method that takes the DDLChange and the conglom on which we are called and invokes the methods
     * above to add factories for the parent or child table as appropriate.
     */
    public void handleDDLChange(DDLChange ddlChange, long onConglomerateNumber) {
        // We are configuring a write context on the PARENT base-table or unique-index.
        AddForeignKeyDDLDescriptor d = (AddForeignKeyDDLDescriptor) ddlChange.getTentativeDDLDesc();
        if (onConglomerateNumber == d.getReferencedConglomerateNumber()) {
            addParentCheckWriteFactory(d.getBackingIndexFormatIds(), d.getReferencedTableVersion());
            addParentInterceptWriteFactory(d.getReferencedTableName(), ImmutableList.of(d.getReferencingConglomerateNumber()));
        }
        // We are configuring a write context on the CHILD fk backing index.
        if (onConglomerateNumber == d.getReferencingConglomerateNumber()) {
            addChildCheck(d.getFkConstraintInfo());
            addChildIntercept(d.getReferencedConglomerateNumber(), d.getFkConstraintInfo());
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // build factories from constraints (used when we are creating the FK from metadata on context startup)
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


    /* Add factories for intercepting writes to FK backing indexes. */
    public void buildForeignKeyInterceptWriteFactory(DataDictionary dataDictionary, ForeignKeyConstraintDescriptor fkConstraintDesc) throws StandardException {
        ReferencedKeyConstraintDescriptor referencedConstraint = fkConstraintDesc.getReferencedConstraint();
        long referencedKeyConglomerateNum;
        // FK references unique constraint.
        if (referencedConstraint.getConstraintType() == DataDictionary.UNIQUE_CONSTRAINT) {
            referencedKeyConglomerateNum = referencedConstraint.getIndexConglomerateDescriptor(dataDictionary).getConglomerateNumber();
        }
        // FK references primary key constraint.
        else {
            referencedKeyConglomerateNum = referencedConstraint.getTableDescriptor().getHeapConglomerateId();
        }
        addChildIntercept(referencedKeyConglomerateNum, new FKConstraintInfo(fkConstraintDesc));
        addChildCheck(new FKConstraintInfo(fkConstraintDesc));
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
        addParentCheckWriteFactory(backingIndexFormatIds, parentTableVersion);
        List<Long> backingIndexConglomIds = DataDictionaryUtils.getBackingIndexConglomerateIdsForForeignKeys(fks);
        addParentInterceptWriteFactory(parentTableName, backingIndexConglomIds);
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
        return Lists.newArrayList(childInterceptWriteFactories.values());
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
