package com.splicemachine.derby.ddl;

import com.splicemachine.pipeline.ddl.TentativeDDLDesc;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class AddForeignKeyDDLDescriptor implements TentativeDDLDesc {

    /* formatIds for the backing index of the FK we are creating */
    private int[] backingIndexFormatIds;
    /* conglom ID of unique index or base table primary key our FK references */
    private long referencedConglomerateId;
    /* conglom ID of the backing-index associated with the FK */
    private long referencingConglomerateId;
    /* users visible name of the table new FK references */
    private String referencedTableName;
    /* Referenced table's encoding version ('1.0', '2.0', etc) */
    private String referencedTableVersion;

    public AddForeignKeyDDLDescriptor() {
    }

    public AddForeignKeyDDLDescriptor(int[] backingIndexFormatIds, long referencedConglomerateId,
                                      String referencedTableName, String referencedTableVersion,
                                      long referencingConglomerateId) {
        this.backingIndexFormatIds = backingIndexFormatIds;
        this.referencedConglomerateId = referencedConglomerateId;
        this.referencedTableName = referencedTableName;
        this.referencedTableVersion = referencedTableVersion;
        this.referencingConglomerateId = referencingConglomerateId;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public int[] getBackingIndexFormatIds() {
        return backingIndexFormatIds;
    }

    public long getReferencedConglomerateId() {
        return referencedConglomerateId;
    }

    public String getReferencedTableName() {
        return referencedTableName;
    }

    public String getReferencedTableVersion() {
        return referencedTableVersion;
    }

    public long getReferencingConglomerateId() {
        return referencingConglomerateId;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Override
    public long getBaseConglomerateNumber() {
        return 0;
    }

    @Override
    public long getConglomerateNumber() {
        return 0;
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(referencedConglomerateId);
        out.writeLong(referencingConglomerateId);
        out.writeUTF(referencedTableName);
        out.writeUTF(referencedTableVersion);
        out.writeInt(backingIndexFormatIds.length);
        for (int formatId : backingIndexFormatIds) {
            out.writeInt(formatId);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.referencedConglomerateId = in.readLong();
        this.referencingConglomerateId = in.readLong();
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
