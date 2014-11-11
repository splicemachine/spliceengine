package com.splicemachine.derby.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SIFactoryDriver;

 public abstract class BaseAllocatedFilter<Data> extends FilterBase implements Writable {
        @SuppressWarnings("unused")
		private static final long serialVersionUID = 2l;
        protected byte[] addressMatch;
        protected boolean foundMatch;
        protected static final SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();

        public BaseAllocatedFilter() {
            super();
        }

        public BaseAllocatedFilter(byte[] localAddress) {
            this.addressMatch = localAddress;
            this.foundMatch=false;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(addressMatch.length);
            out.write(addressMatch);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            addressMatch = new byte[in.readInt()];
            in.readFully(addressMatch);
        }        
        public ReturnCode internalFilter(Data ignored) {
            if(foundMatch)
                return ReturnCode.NEXT_ROW; //can skip the remainder, because we've already got an entry allocated
            byte[] value = dataLib.getDataValue(ignored);
            if(Bytes.equals(addressMatch,value)){
                foundMatch= true;
                return ReturnCode.INCLUDE;
            }else if(value.length!=0 || Bytes.equals(value,SIConstants.COUNTER_COL)){
                //a machine has already got this id -- also skip the counter column, since we don't need that
                return ReturnCode.SKIP;
            }
            return ReturnCode.INCLUDE; //this is an available entry
        }
    }