/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.hbase;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.si.constants.SIConstants;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AllocatedFilter extends FilterBase implements Writable{
    @SuppressWarnings("unused")
    private static final long serialVersionUID=2l;
    protected byte[] addressMatch;
    protected boolean foundMatch;

    @SuppressWarnings("unused") //serialization constructor: REQUIRED
    public AllocatedFilter(){
        super();
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public AllocatedFilter(byte[] localAddress){
        this.addressMatch=localAddress;
        this.foundMatch=false;
    }

    @Override
    public void write(DataOutput out) throws IOException{
        out.writeInt(addressMatch.length);
        out.write(addressMatch);
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        addressMatch=new byte[in.readInt()];
        in.readFully(addressMatch);
    }

    @Override
    public ReturnCode filterKeyValue(Cell ignored){
        if(foundMatch)
            return ReturnCode.NEXT_ROW; //can skip the remainder, because we've already got an entry allocated
        byte[] value=ignored.getValueArray();
        int offset=ignored.getValueOffset();
        int length=ignored.getValueLength();
        if(Bytes.equals(addressMatch,0,addressMatch.length,value,offset,length)){
            foundMatch=true;
            return ReturnCode.INCLUDE;
        }else if(value.length!=0
                || Bytes.equals(value,offset,length,SIConstants.COUNTER_COL,0,SIConstants.COUNTER_COL.length)){
            //a machine has already got this id -- also skip the counter column, since we don't need that
            return ReturnCode.SKIP;
        }
        return ReturnCode.INCLUDE; //this is an available entry
    }

    /**
     * @return The filter serialized using pb
     */
    public byte[] toByteArray(){
        SpliceMessage.AllocateFilterMessage.Builder builder= SpliceMessage.AllocateFilterMessage.newBuilder();
        if(this.addressMatch!=null) builder.setAddressMatch(ZeroCopyLiteralByteString.wrap(this.addressMatch));
        return builder.build().toByteArray();
    }

    /**
     * @param addressMatch A pb serialized {@code AllocatedFilter} instance
     * @return An instance of {@code BaseAllocatedFilter} made from <code>bytes</code>
     * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
     * @see #toByteArray
     */
    @SuppressWarnings("unused") //Deserialization method-- REQUIRED
    public static AllocatedFilter parseFrom(final byte[] addressMatch) throws DeserializationException{
        SpliceMessage.AllocateFilterMessage proto;
        try{
            proto=SpliceMessage.AllocateFilterMessage.parseFrom(addressMatch);
        }catch(InvalidProtocolBufferException e){
            throw new DeserializationException(e);
        }
        return new AllocatedFilter(proto.hasAddressMatch()?proto.getAddressMatch().toByteArray():null);
    }
}