package com.splicemachine.derby.impl.load;

import com.splicemachine.pipeline.exception.ErrorState;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.SQLDecimal;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Types;

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
    private int decimalDigits;
    private String columnDefault;

    public int getInsertPos() {
        return insertPos;
    }

    public void setInsertPos(int insertPos) {
        this.insertPos = insertPos;
    }

    private int insertPos = -1;
    private String formatStr;
    private boolean isFormatStrSet;
		//autoincrement stuff
		private byte[] sequenceRowLocation; //can be null
		private long autoIncrementStart;
		private long autoIncrementIncrement;

    @Deprecated
    public ColumnContext(){}

    private ColumnContext(int colNumber,
                          int colType,
                          int pkPos,
                          boolean isNullable,
                          String colName,
                          int length,
                          int decimalDigits,
                          String columnDefault,
													long autoIncrementStart,
													long autoIncrementIncrement,
													byte[] sequenceRowLocation) {
        this.colNumber = colNumber;
        this.columnType = colType;
        this.isNullable = isNullable;
        this.pkPos = pkPos;
        this.colName = colName;
        this.length = length;
        this.decimalDigits = decimalDigits;
        this.columnDefault = columnDefault;
				this.autoIncrementIncrement = autoIncrementIncrement;
				this.autoIncrementStart = autoIncrementStart;
				this.sequenceRowLocation = sequenceRowLocation;
				this.isFormatStrSet = false;
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
        out.writeBoolean(decimalDigits>0);
        if (decimalDigits>0) 
        	out.writeInt(decimalDigits);
        out.writeBoolean(columnDefault!=null);

        if (columnDefault != null) 
        	out.writeUTF(columnDefault);
				out.writeBoolean(sequenceRowLocation!=null);
				if(sequenceRowLocation!=null){
						out.writeInt(sequenceRowLocation.length);
						out.write(sequenceRowLocation);
						out.writeLong(autoIncrementStart);
						out.writeLong(autoIncrementIncrement);
				}

            out.writeInt(insertPos);


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
        if(in.readBoolean())
        	decimalDigits = in.readInt();
        if (in.readBoolean())
        	columnDefault = in.readUTF();
				if(in.readBoolean()){
						sequenceRowLocation = new byte[in.readInt()];
						in.readFully(sequenceRowLocation);
						autoIncrementStart = in.readLong();
						autoIncrementIncrement = in.readLong();
				}
        this.insertPos = in.readInt();
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
    
    public int getDecimalDigits() {
    	return decimalDigits;
    }

    public String getFormatStr() {
    	return formatStr;
    }

		public byte[] getSequenceRowLocation() {
				return sequenceRowLocation;
		}

		public long getAutoIncrementStart() {
				return autoIncrementStart;
		}

		public long getAutoIncrementIncrement() {
				return autoIncrementIncrement;
		}

		public boolean isAutoIncrement(){
				return sequenceRowLocation!=null;
		}

		public void setFormatStr(String fmst) {
    	this.formatStr = fmst;
    	this.isFormatStrSet = true;
    }

    public boolean isFormatStrSet () {
    	return isFormatStrSet;
    }
		public String getColumnDefault() {
				return columnDefault;
		}
    
    public void validate(DataValueDescriptor column) throws StandardException {
        if(!isNullable && column.isNull())
           throw ErrorState.LANG_NULL_INTO_NON_NULL.newException(colName);
        else if(length>0 && column instanceof StringDataValue){
           //must be a string data type
            StringDataValue sdv = (StringDataValue)column;
            if(sdv.getLength()>length)
                throw ErrorState.LANG_STRING_TRUNCATION.newException(column.getTypeName(),column.getString(),length);
        } else if (columnType == Types.DECIMAL) {
        	SQLDecimal d = (SQLDecimal)column;
        	d.setWidth(length, decimalDigits, true);
        }
    }

    public static class Builder{
        private int columnType;
        private int colNumber;
        private boolean isNullable = true;
        private int pkPos = -1;
        private String colName;
        private int length = -1;
        private int decimalDigits;
        private String columnDefault;
				private long autoincStart = -1l;
				private long autoincIncrement = -1l;
				private byte[] sequenceRowLocation;

        public Builder autoIncrementStart(long autoincStart){
						this.autoincStart = autoincStart;
						return this;
				}
				public Builder autoIncrementIncrement(long autoincIncrement){
						this.autoincIncrement = autoincIncrement;
						return this;
				}

				public boolean isAutoIncrement(){
						return autoincStart>=0;
				}

				public Builder sequenceRowLocation(byte[] sequenceRowLocation){
						this.sequenceRowLocation = sequenceRowLocation;
						return this;
				}

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

        public Builder decimalDigits(int decimalDigits) {
        	this.decimalDigits = decimalDigits;
        	return this;
        }

        public Builder columnDefault(String columnDefault) {
        	this.columnDefault = columnDefault;
        	return this;
        }

        public boolean isNullable() {
            return isNullable;
        }

        public ColumnContext build(){
            return new ColumnContext(colNumber, columnType,pkPos,isNullable,colName,length, decimalDigits, columnDefault,autoincStart,autoincIncrement,sequenceRowLocation);
        }

				public int getColumnNumber() { return colNumber; }

    }
}
