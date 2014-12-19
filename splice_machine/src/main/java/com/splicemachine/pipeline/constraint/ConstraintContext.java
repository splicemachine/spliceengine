package com.splicemachine.pipeline.constraint;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.encoding.Encoding;
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

		public ConstraintContext(String tableName, String constraintName){
				this.tableName = tableName;
				this.constraintName = constraintName;
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

		public void write(Output output) throws IOException{
				output.writeBoolean(tableName!=null);
				if(tableName!=null){
						output.writeString(tableName);
				}

				output.writeBoolean(constraintName!=null);
				if(constraintName!=null)
						output.writeString(constraintName);
		}

		public static ConstraintContext fromBytes(Input input) throws IOException{
				String tableName = input.readBoolean()?input.readString():null;
				String constraintName = input.readBoolean()?input.readString(): null;
				return new ConstraintContext(tableName,constraintName);
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


    public int encodedLength() {
        int length = 1; //one byte to store the nullability
        if(tableName!=null)
            length+=tableName.length()+Encoding.encodedLength(tableName.length());
        if(constraintName!=null)
            length+=constraintName.length()+Encoding.encodedLength(constraintName.length());
        return length;
    }

    public int encodeInto(byte[] data, int offset){
        byte nullByte = 0x00;
        if(tableName!=null)
            nullByte |= 0x10;
        if(constraintName!=null)
            nullByte |= 0x01;

        int l = 0;
        data[offset] = nullByte;
        l++; offset++;
        if(tableName!=null) {
            int len = Encoding.encode(tableName.length(),data,offset,false);
            l += len;
            offset += len;
            len = Encoding.encodeInto(tableName, data, offset);
            l += len;
            offset += len;
        }
        if(constraintName!=null) {
            int len = Encoding.encode(constraintName.length(),data,offset,false);
            l += len;
            offset += len;
            len = Encoding.encodeInto(constraintName, data, offset);
            l += len;
        }
        return l;
    }

    public static ConstraintContext decode(byte[] data, int offset,long[] lengthHolder){
        int len = 1;
        byte nullByte = data[offset];
        offset++;
        boolean decodeTable = (nullByte & 0x10)!=0;
        boolean decodeCName = (nullByte & 0x01)!=0;
        String tableName = null;
        String constraintName = null;
        if(decodeTable) {
            Encoding.decodeLongWithLength(data,offset,false,lengthHolder);
            int l = (int)lengthHolder[0];
            offset+=lengthHolder[1];
            len+=lengthHolder[1];
            tableName = Encoding.decodeString(data, offset, l,false);
            offset+=l;
            len+=l;
        }if(decodeCName){
            Encoding.decodeLongWithLength(data,offset,false,lengthHolder);
            int l = (int)lengthHolder[0];
            offset+=lengthHolder[1];
            len+=lengthHolder[1];
            constraintName = Encoding.decodeString(data, offset, l,false);
            offset+=l;
            len+=l;
        }
        lengthHolder[0] = len;
        lengthHolder[1] = offset;

        return new ConstraintContext(tableName,constraintName);
    }
}
