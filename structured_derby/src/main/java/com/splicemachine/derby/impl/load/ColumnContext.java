package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.utils.ErrorState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.StringDataValue;

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
    private int length;

    @Deprecated
    public ColumnContext(){}

    private ColumnContext(int colNumber,
                          int colType,
                          int pkPos,
                          boolean isNullable,
                          String colName,
                          int length){
        this.colNumber = colNumber;
        this.columnType = colType;
        this.isNullable = isNullable;
        this.pkPos = pkPos;
        this.colName = colName;
        this.length = length;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(colName);
        out.writeInt(colNumber);
        out.writeInt(columnType);
        out.writeInt(pkPos);
        out.writeBoolean(isNullable);
        out.writeBoolean(length>0);
        if(length>0)
            out.writeInt(length);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.colName = in.readUTF();
        this.colNumber = in.readInt();
        this.columnType = in.readInt();
        this.pkPos = in.readInt();
        this.isNullable = in.readBoolean();
        if(in.readBoolean())
            length = in.readInt();
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

    public void validate(DataValueDescriptor column) throws StandardException {
        if(!isNullable && column.isNull())
           throw ErrorState.LANG_NULL_INTO_NON_NULL.newException(colName);
        else if(length>0 && column instanceof StringDataValue){
           //must be a string data type
            StringDataValue sdv = (StringDataValue)column;
            if(sdv.getLength()>length)
                throw ErrorState.LANG_STRING_TRUNCATION.newException(column.getTypeName(),column.getString(),length);
        }
    }

    public static class Builder{
        private int columnType;
        private int colNumber;
        private boolean isNullable = true;
        private int pkPos = -1;
        private String colName;
        private int length = -1;

        public Builder length(int length){
            this.length = length;
            return this;
        }

        public String getColumnName(){
            return colName;
        }

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
            return new ColumnContext(colNumber, columnType,pkPos,isNullable,colName,length);
        }
    }
}
