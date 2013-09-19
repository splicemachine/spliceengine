package com.splicemachine.derby.impl.load;

import com.google.common.base.Preconditions;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 * Created on: 9/19/13
 */
public class ColumnContext implements Externalizable {
    private static final long serialVersionUID = 2l;

    private int colNumber;
    private int columnType;
    private int pkPos;
    private boolean isNullable;
    private String colName;

    @Deprecated
    public ColumnContext(){}

    private ColumnContext(int colNumber,int colType,int pkPos,boolean isNullable,String colName){
        this.colNumber = colNumber;
        this.columnType = colType;
        this.isNullable = isNullable;
        this.pkPos = pkPos;
        this.colName = colName;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(colName);
        out.writeInt(colNumber);
        out.writeInt(columnType);
        out.writeInt(pkPos);
        out.writeBoolean(isNullable);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.colName = in.readUTF();
        this.colNumber = in.readInt();
        this.columnType = in.readInt();
        this.pkPos = in.readInt();
        this.isNullable = in.readBoolean();
    }

    public int getColumnNumber() {
        return colNumber;
    }

    public int getColumnType() {
        return columnType;
    }

    public boolean isPkColumn() {
        return pkPos>=0;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public int getPkPosition() {
        return pkPos;
    }

    public String getColumnName() {
        return colName;
    }

    public static class Builder{
        private int columnType;
        private int colNumber;
        private boolean isNullable = true;
        private int pkPos = -1;
        private String colName;

        public Builder columnName(String columnName){
            this.colName = columnName;
            return this;
        }

        public Builder columnNumber(int columnNumber){
            this.colNumber = columnNumber;
            return this;
        }

        public Builder columnType(int columnType) {
            this.columnType = columnType;
            return this;
        }

        public Builder primaryKeyPos(int pkPos){
            this.pkPos = pkPos;
            return this;
        }

        public Builder nullable(boolean nullable) {
            isNullable = nullable;
            return this;
        }

        public ColumnContext build(){
            return new ColumnContext(colNumber, columnType,pkPos,isNullable,colName);
        }
    }
}
