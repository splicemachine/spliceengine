/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.dvd;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.CharacterStreamDescriptor;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;
import com.splicemachine.db.iapi.util.StringUtil;
import com.splicemachine.encoding.Encoding;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;

import java.io.IOException;
import java.io.ObjectInput;
import java.text.RuleBasedCollator;

public abstract class LazyStringDataValueDescriptor extends LazyDataValueDescriptor<StringDataValue> implements StringDataValue{

    public LazyStringDataValueDescriptor(){ }

    public LazyStringDataValueDescriptor(StringDataValue sdv){
        init(sdv);
    }

    protected StringDataValue unwrap(StringDataValue sdv){
        StringDataValue unwrapped;

        if(sdv instanceof LazyStringDataValueDescriptor){
            LazyStringDataValueDescriptor ldvd=(LazyStringDataValueDescriptor)sdv;
            ldvd.forceDeserialization();
            unwrapped=ldvd.dvd;
        }else{
            unwrapped=sdv;
        }

        return unwrapped;
    }

    @Override
    public void normalize(DataTypeDescriptor dtd,DataValueDescriptor source) throws StandardException{
        if(!source.isLazy()){
            forceDeserialization();
            dvd.normalize(dtd,source);
            resetForSerialization();
        }else{
            LazyStringDataValueDescriptor ldvd=(LazyStringDataValueDescriptor)source;
            if(ldvd.isDeserialized()){
                forceDeserialization();
                dvd.normalize(dtd,source);
                resetForSerialization();
            }else{
                normalizeBytes(dtd,ldvd);
            }
        }
    }


    @Override
    public boolean isDoubleType(){
        return false;
    }

    @Override
    public DataValueFactoryImpl.Format getFormat(){
        return Format.VARCHAR;
    }

    @Override
    public StringDataValue concatenate(StringDataValue leftOperand,StringDataValue rightOperand,StringDataValue result) throws StandardException{
        forceDeserialization();
        return dvd.concatenate(unwrap(leftOperand),unwrap(rightOperand),result);
    }

    @Override
    public BooleanDataValue like(DataValueDescriptor pattern) throws StandardException{
        forceDeserialization();
        return dvd.like(unwrap(pattern));
    }

    @Override
    public BooleanDataValue like(DataValueDescriptor pattern,DataValueDescriptor escape) throws StandardException{
        forceDeserialization();
        return dvd.like(unwrap(pattern),unwrap(escape));
    }

    @Override
    public StringDataValue ansiTrim(int trimType,StringDataValue trimChar,StringDataValue result) throws StandardException{
        forceDeserialization();
        return dvd.ansiTrim(trimType,trimChar,result);
    }

    @Override
    public StringDataValue upper(StringDataValue result) throws StandardException{
        forceDeserialization();
        return dvd.upper(unwrap(result));
    }

    @Override
    public StringDataValue lower(StringDataValue result) throws StandardException{
        forceDeserialization();
        return dvd.lower(unwrap(result));
    }

    @Override
    public NumberDataValue locate(StringDataValue searchFrom,NumberDataValue start,NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dvd.locate(searchFrom,start,result);
    }

    @Override
    public char[] getCharArray() throws StandardException{
        forceDeserialization();
        return dvd.getCharArray();
    }

    @Override
    public StringDataValue getValue(RuleBasedCollator collatorForComparison){
        forceDeserialization();
        return dvd.getValue(collatorForComparison);
    }

    @Override
    public StreamHeaderGenerator getStreamHeaderGenerator(){
        forceDeserialization();
        return dvd.getStreamHeaderGenerator();
    }

    @Override
    public void setStreamHeaderFormat(Boolean usePreTenFiveHdrFormat){
        forceDeserialization();
        dvd.setStreamHeaderFormat(usePreTenFiveHdrFormat);
    }

    @Override
    public CharacterStreamDescriptor getStreamWithDescriptor() throws StandardException{
        forceDeserialization();
        return dvd.getStreamWithDescriptor();
    }

    @Override
    public NumberDataValue charLength(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dvd.charLength(result);
    }

    @Override
    public ConcatableDataValue substring(NumberDataValue start,NumberDataValue length,ConcatableDataValue result,int maxLen) throws StandardException{
        forceDeserialization();
        return dvd.substring(start,length,result,maxLen);
    }

    @Override
    public ConcatableDataValue replace(StringDataValue fromStr,StringDataValue toStr,ConcatableDataValue result) throws StandardException{
        forceDeserialization();
        return dvd.replace(fromStr,toStr,result);
    }

    @Override
    public void setWidth(int desiredWidth,int desiredScale,boolean errorOnTrunc) throws StandardException{
        forceDeserialization();
        dvd.setWidth(desiredWidth,desiredScale,errorOnTrunc);
        resetForSerialization();
    }

    @Override
    public DataValueDescriptor recycle(){
        restoreToNull();
        return this;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{

        super.readExternal(in);

        init(dvd);
    }

    @Override
    public String toString(){
        try{
            return getString();
        }catch(StandardException e){
            throw new RuntimeException(e);
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void normalizeBytes(DataTypeDescriptor dtd,LazyStringDataValueDescriptor ldvd) throws StandardException{
        int desiredWidth=dtd.getMaximumWidth();
        byte[] sourceBytes=ldvd.getBytes();
        byte[] result=sourceBytes;
        int sourceWidth=sourceBytes.length;

        if(sourceWidth>desiredWidth){
            // normalize the byte array to the desired length
            if(ldvd.descendingOrder)
                checkTruncatedBytesDescending(desiredWidth,sourceBytes,sourceWidth);
            else
                checkTruncatedBytesAscending(desiredWidth,sourceBytes,sourceWidth);
            if(desiredWidth==0){
                //only possible string is the empty string, which is different than what we have here
                result = Encoding.encode("",ldvd.descendingOrder);
            }else{
                result=new byte[desiredWidth];
                System.arraycopy(sourceBytes,0,result,0,desiredWidth);
            }
        }
        initForDeserialization(tableVersion,serializer,result,0,result.length,ldvd.descendingOrder);
    }

    /*
     * Check for truncated bytes
     */
    private void checkTruncatedBytesAscending(int desiredWidth,byte[] sourceBytes,int sourceWidth) throws StandardException{
        for(int posn=desiredWidth;posn<sourceWidth;posn++){
            if(sourceBytes[posn]!=(byte)0x22){
                forceDeserialization();
                throw StandardException.newException(
                        SQLState.LANG_STRING_TRUNCATION,
                        getTypeName(),
                        StringUtil.formatForPrint(getString()),
                        String.valueOf(desiredWidth));
            }
        }
    }

    private void checkTruncatedBytesDescending(int desiredWidth,byte[] sourceBytes,int sourceWidth) throws StandardException{
        for(int posn=desiredWidth;posn<sourceWidth;posn++){
            if(sourceBytes[posn]!=(byte)0xDD){
                forceDeserialization();
                throw StandardException.newException(
                        SQLState.LANG_STRING_TRUNCATION,
                        getTypeName(),
                        StringUtil.formatForPrint(getString()),
                        String.valueOf(desiredWidth));
            }
        }
    }

    @Override
    public void decodeFromKey(PositionedByteRange builder) throws StandardException {
        forceDeserialization();
        dvd.decodeFromKey(builder);
    }

    @Override
    public void encodeIntoKey(PositionedByteRange builder, Order order) throws StandardException {
        forceDeserialization();
        dvd.encodeIntoKey(builder,order);
    }

    @Override
    public int encodedKeyLength() throws StandardException {
        forceDeserialization();
        return dvd.encodedKeyLength();
    }

    @Override
    public void read(UnsafeRow unsafeRow, int ordinal) throws StandardException {
        forceDeserialization();
        dvd.read(unsafeRow, ordinal);
    }

    @Override
    public void write(UnsafeRowWriter unsafeRowWriter, int ordinal) throws StandardException {
        forceDeserialization();
        dvd.write(unsafeRowWriter, ordinal);
    }
}
