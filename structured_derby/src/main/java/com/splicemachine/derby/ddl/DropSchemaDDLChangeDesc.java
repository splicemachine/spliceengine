package com.splicemachine.derby.ddl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.pipeline.ddl.TentativeDDLDesc;

/**
 * @author Scott Fines
 *         Date: 10/7/14
 */
public class DropSchemaDDLChangeDesc implements TentativeDDLDesc{
    private String schemaName;

    public DropSchemaDDLChangeDesc() { }

    public DropSchemaDDLChangeDesc(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override public long getBaseConglomerateNumber() { return 0; }
    @Override public long getConglomerateNumber() { return 0; }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(schemaName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        schemaName = in.readUTF();
    }
}
