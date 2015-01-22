package com.splicemachine.pipeline.constraint;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.*;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Immutable class representing a named constraint on a named table.
 */
public class ConstraintContext implements Externalizable {

    /* The message args which will be passed to our StandardException factory method for creating a constraint
     * violation message appropriate for the constraint. */
    private String[] messageArgs;

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // convenient factory methods for various types of constraints
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public static ConstraintContext empty() {
        return new ConstraintContext(null);
    }

    public static ConstraintContext unique(ConstraintDescriptor cd) {
        String tableName = cd.getTableDescriptor().getName();
        String constraintName = cd.getConstraintName();
        return new ConstraintContext(constraintName, tableName);
    }

    public static ConstraintContext unique(TableDescriptor td, ConglomerateDescriptor conglomDesc) {
        String tableName = td.getName();
        String constraintName = conglomDesc.getConglomerateName();
        return new ConstraintContext(constraintName, tableName);
    }

    public static ConstraintContext primaryKey(ConstraintDescriptor cDescriptor) {
        String tableName = cDescriptor.getTableDescriptor().getName();
        String constraintName = cDescriptor.getConstraintName();
        return new ConstraintContext(constraintName, tableName);
    }

    public static ConstraintContext foreignKey(ForeignKeyConstraintDescriptor fkConstraintDesc) throws StandardException {
        String tableName = fkConstraintDesc.getTableDescriptor().getName();
        String constraintName = fkConstraintDesc.getConstraintName();
        ColumnDescriptorList columnDescriptors = fkConstraintDesc.getColumnDescriptors();
        String columnNames = Joiner.on(",").join(Lists.transform(columnDescriptors, new ColumnDescriptorNameFunction()));
        return new ConstraintContext(constraintName, tableName, "Operation", "(" + columnNames + ")");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /* For serialization */
    @Deprecated
    public ConstraintContext() {
    }

    /* Use factory methods above instead for clarity */
    public ConstraintContext(String... messageArgs) {
        this.messageArgs = messageArgs;
    }

    public String[] getMessages() {
        return messageArgs;
    }

    @Override
    public boolean equals(Object o) {
        return (this == o) || (o instanceof ConstraintContext) &&
                Arrays.equals(this.messageArgs, ((ConstraintContext) o).messageArgs);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(messageArgs);
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        short len = objectInput.readShort();
        if (len > 0) {
            messageArgs = new String[len];
        }
        for (int i = 0; i < messageArgs.length; i++) {
            messageArgs[i] = objectInput.readUTF();
        }
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        short len = (short) (messageArgs == null ? 0 : messageArgs.length);
        objectOutput.writeShort(len);
        for (int i = 0; i < len; i++) {
            objectOutput.writeUTF(messageArgs[i]);
        }
    }

    @Override
    public String toString() {
        return "ConstraintContext{" +
                "messageArgs=" + Arrays.toString(messageArgs) +
                '}';
    }

    private static class ColumnDescriptorNameFunction implements Function<ColumnDescriptor, String> {
        @Override
        public String apply(ColumnDescriptor columnDescriptor) {
            return columnDescriptor.getColumnName();
        }
    }

}
