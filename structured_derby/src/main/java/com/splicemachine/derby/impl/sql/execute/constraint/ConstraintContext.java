package com.splicemachine.derby.impl.sql.execute.constraint;

import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class ConstraintContext implements Externalizable {

    private String tableName;
    private String constraintName;

    @Deprecated
    public ConstraintContext(){};

    public ConstraintContext(ConstraintDescriptor cd){
        tableName = cd.getTableDescriptor().getName();
        constraintName = cd.getConstraintName();
    }

    public ConstraintContext(TableDescriptor td, ConglomerateDescriptor cd){
        tableName = td.getName();
        constraintName = cd.getConglomerateName();
    }

    public String getTableName() {
        return tableName;
    }

    public String getConstraintName() {
        return constraintName;
    }


    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {

        objectOutput.writeBoolean(tableName != null);

        if(tableName != null){
            objectOutput.writeUTF(tableName);
        }

        objectOutput.writeBoolean(constraintName != null);

        if(constraintName != null){
            objectOutput.writeUTF(constraintName);
        }
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {

        if(objectInput.readBoolean()){
            tableName = objectInput.readUTF();
        }

        if(objectInput.readBoolean()){
            constraintName = objectInput.readUTF();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConstraintContext)) return false;

        ConstraintContext that = (ConstraintContext) o;

        return constraintName.equals(that.constraintName) && tableName.equals(that.tableName);

    }

    @Override
    public int hashCode() {
        int result = tableName.hashCode();
        result = 31 * result + constraintName.hashCode();
        return result;
    }
}
