package com.splicemachine.pipeline.writecontextfactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A light externalizable version of ForeignKeyConstraintDescriptor
 */
public class FKConstraintInfo implements Externalizable {

    /* The name of this foreign key constraint. For example 'FK_1' or 'SQL43453453' */
    private String constraintName;

    /* User visible table name upon which the FK is defined. */
    private String tableName;

    /* Comma separated list of column names upon which this FK is defined. */
    private String columnNames;

    /* for serialization */
    public FKConstraintInfo() {
    }

    /**
     * Create from a ForeignKeyConstraintDescriptor
     */
    public FKConstraintInfo(ForeignKeyConstraintDescriptor fKConstraintDescriptor) {
        this.tableName = fKConstraintDescriptor.getTableDescriptor().getName();
        this.constraintName = fKConstraintDescriptor.getConstraintName();
        ColumnDescriptorList columnDescriptors = fKConstraintDescriptor.getColumnDescriptors();
        this.columnNames = Joiner.on(",").join(Lists.transform(columnDescriptors, new ColumnDescriptorNameFunction()));
    }


    public String getTableName() {
        return tableName;
    }

    public String getConstraintName() {
        return constraintName;
    }

    public String getColumnNames() {
        return columnNames;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(constraintName);
        out.writeObject(tableName);
        out.writeObject(columnNames);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.constraintName = (String) in.readObject();
        this.tableName = (String) in.readObject();
        this.columnNames = (String) in.readObject();
    }


    private static class ColumnDescriptorNameFunction implements Function<ColumnDescriptor, String> {
        @Override
        public String apply(ColumnDescriptor columnDescriptor) {
            return columnDescriptor.getColumnName();
        }
    }
}
