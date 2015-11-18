package com.splicemachine.pipeline.writecontextfactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.derby.utils.DataDictionaryUtils;
import com.splicemachine.protobuf.ProtoUtil;
import org.sparkproject.guava.primitives.Ints;
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
        parentCheckWriteFactory = new ForeignKeyParentCheckWriteFactory(backingIndexFormatIds, parentTableVersion);
    }

    public void addParentInterceptWriteFactory(String parentTableName, List<Long> backingIndexConglomIds) {
        /* One instance handles all FKs that reference this primary key or unique index */
        if (parentInterceptWriteFactory == null) {
            parentInterceptWriteFactory = new ForeignKeyParentInterceptWriteFactory(parentTableName, backingIndexConglomIds);
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
    public void handleForeignKeyAdd(DDLChange ddlChange, long onConglomerateNumber) {
        TentativeFK tentativeFKAdd = ddlChange.getTentativeFK();
        // We are configuring a write context on the PARENT base-table or unique-index.
        if (onConglomerateNumber == tentativeFKAdd.getReferencedConglomerateNumber()) {
            addParentCheckWriteFactory(Ints.toArray(tentativeFKAdd.getBackingIndexFormatIdsList()), tentativeFKAdd.getReferencedTableVersion());
            addParentInterceptWriteFactory(tentativeFKAdd.getReferencedTableName(), ImmutableList.of(tentativeFKAdd.getReferencingConglomerateNumber()));
        }
        // We are configuring a write context on the CHILD fk backing index.
        if (onConglomerateNumber == tentativeFKAdd.getReferencingConglomerateNumber()) {
            addChildCheck(tentativeFKAdd.getFkConstraintInfo());
            addChildIntercept(tentativeFKAdd.getReferencedConglomerateNumber(), tentativeFKAdd.getFkConstraintInfo());
        }
    }

    public void handleForeignKeyDrop(DDLChange ddlChange, long onConglomerateNumber) {
        TentativeFK tentativeFKAdd = ddlChange.getTentativeFK();
        // We are configuring a write context on the PARENT base-table or unique-index.
        if (onConglomerateNumber == tentativeFKAdd.getReferencedConglomerateNumber()) {
            parentInterceptWriteFactory.removeReferencingIndexConglomerateNumber(tentativeFKAdd.getReferencingConglomerateNumber());
        }
        // We are configuring a write context on the CHILD fk backing index.
        if (onConglomerateNumber == tentativeFKAdd.getReferencingConglomerateNumber()) {
            childInterceptWriteFactories.remove(tentativeFKAdd.getReferencedConglomerateNumber());
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
        FKConstraintInfo info = ProtoUtil.createFKConstraintInfo(fkConstraintDesc);
        addChildIntercept(referencedKeyConglomerateNum, info);
        addChildCheck(info);
    }

    /* Add factories for *checking* existence of FK referenced primary-key or unique-index rows. */
    public void buildForeignKeyCheckWriteFactory(ReferencedKeyConstraintDescriptor cDescriptor) throws StandardException {
        ConstraintDescriptorList fks = cDescriptor.getForeignKeyConstraints(ConstraintDescriptor.ENABLED);
        if (fks.isEmpty()) {
            return;
        }
        ColumnDescriptorList backingIndexColDescriptors = cDescriptor.getColumnDescriptors();
        int backingIndexFormatIds[] = backingIndexColDescriptors.getFormatIds();
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
