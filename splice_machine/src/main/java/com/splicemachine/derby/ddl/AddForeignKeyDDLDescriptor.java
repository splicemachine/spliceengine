package com.splicemachine.derby.ddl;

import com.splicemachine.pipeline.ddl.TentativeDDLDesc;
import com.splicemachine.pipeline.writecontextfactory.FKConstraintInfo;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class AddForeignKeyDDLDescriptor implements TentativeDDLDesc {

    /* Info about the constraint that is ultimately used in constraint violation error messages */
    private FKConstraintInfo fkConstraintInfo;
    /* formatIds for the backing index of the FK we are creating */
    private int[] backingIndexFormatIds;
    /* conglom number of unique index or base table primary key our FK references */
    private long referencedConglomerateNumber;
    /* conglom number of the backing-index associated with the FK */
    private long referencingConglomerateNumber;
    /* users visible name of the table new FK references */
    private String referencedTableName;
    /* Referenced table's encoding version ('1.0', '2.0', etc) */
    private String referencedTableVersion;

    public AddForeignKeyDDLDescriptor() {
    }

    public AddForeignKeyDDLDescriptor(FKConstraintInfo fkConstraintInfo, int[] backingIndexFormatIds, long referencedConglomerateNumber,
                                      String referencedTableName, String referencedTableVersion,
                                      long referencingConglomerateNumber) {
        this.fkConstraintInfo = fkConstraintInfo;
        this.backingIndexFormatIds = backingIndexFormatIds;
        this.referencedConglomerateNumber = referencedConglomerateNumber;
        this.referencedTableName = referencedTableName;
        this.referencedTableVersion = referencedTableVersion;
        this.referencingConglomerateNumber = referencingConglomerateNumber;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


    public FKConstraintInfo getFkConstraintInfo() {
        return fkConstraintInfo;
    }

    public int[] getBackingIndexFormatIds() {
        return backingIndexFormatIds;
    }

    public long getReferencedConglomerateNumber() {
        return referencedConglomerateNumber;
    }

    public String getReferencedTableName() {
        return referencedTableName;
    }

    public String getReferencedTableVersion() {
        return referencedTableVersion;
    }

    public long getReferencingConglomerateNumber() {
        return referencingConglomerateNumber;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Override
    public long getBaseConglomerateNumber() {
        throw new UnsupportedOperationException("this method from interface not used");
    }

    @Override
    public long getConglomerateNumber() {
        throw new UnsupportedOperationException("this method from interface not used");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(fkConstraintInfo);
        out.writeLong(referencedConglomerateNumber);
        out.writeLong(referencingConglomerateNumber);
        out.writeUTF(referencedTableName);
        out.writeUTF(referencedTableVersion);
        out.writeInt(backingIndexFormatIds.length);
        for (int formatId : backingIndexFormatIds) {
            out.writeInt(formatId);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.fkConstraintInfo = (FKConstraintInfo) in.readObject();
        this.referencedConglomerateNumber = in.readLong();
        this.referencingConglomerateNumber = in.readLong();
        this.referencedTableName = in.readUTF();
        this.referencedTableVersion = in.readUTF();
        int n = in.readInt();
        if (n > 0) {
            backingIndexFormatIds = new int[n];
            for (int i = 0; i < n; ++i) {
                backingIndexFormatIds[i] = in.readInt();
            }
        }
    }

}
