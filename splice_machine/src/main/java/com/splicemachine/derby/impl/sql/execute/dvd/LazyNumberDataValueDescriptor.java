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
import com.splicemachine.db.iapi.types.DataValueFactoryImpl;
import com.splicemachine.db.iapi.types.NumberDataValue;
import com.splicemachine.db.iapi.types.VariableSizeDataValue;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;

import java.math.BigDecimal;

public abstract class LazyNumberDataValueDescriptor extends LazyDataValueDescriptor<NumberDataValue> implements NumberDataValue, VariableSizeDataValue{

    public LazyNumberDataValueDescriptor(){
    }

    public LazyNumberDataValueDescriptor(NumberDataValue ndv){
        init(ndv);
    }

    public void init(NumberDataValue ndv){
        super.init(ndv);
    }

    @Override
    public DataValueFactoryImpl.Format getFormat(){
        forceDeserialization();
        return dvd.getFormat();
    }

    public NumberDataValue plus(NumberDataValue addend1,NumberDataValue addend2,NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dvd.plus(addend1,addend2,result);
    }

    @Override
    public NumberDataValue minus(NumberDataValue left,NumberDataValue right,NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dvd.minus(left,right,result);
    }

    @Override
    public NumberDataValue times(NumberDataValue left,NumberDataValue right,NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dvd.times(left,right,result);
    }

    @Override
    public NumberDataValue divide(NumberDataValue dividend,NumberDataValue divisor,NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dvd.divide(dividend,divisor,result);
    }

    @Override
    public NumberDataValue divide(NumberDataValue dividend,NumberDataValue divisor,NumberDataValue result,int scale) throws StandardException{
        forceDeserialization();
        return dvd.divide(dividend,divisor,result);
    }

    @Override
    public NumberDataValue mod(NumberDataValue dividend,NumberDataValue divisor,NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dvd.mod(dividend,divisor,result);
    }

    @Override
    public NumberDataValue minus(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dvd.minus(result);
    }

    @Override
    public NumberDataValue absolute(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dvd.absolute(result);
    }

    @Override
    public NumberDataValue sqrt(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dvd.sqrt(result);
    }

    @Override
    public void setValue(Number theValue) throws StandardException{
        forceDeserialization();
        dvd.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public int getDecimalValuePrecision(){
        forceDeserialization();
        return dvd.getDecimalValuePrecision();
    }

    @Override
    public int getDecimalValueScale(){
        forceDeserialization();
        return dvd.getDecimalValueScale();
    }

    @Override
    public BigDecimal getBigDecimal() throws StandardException{
        forceDeserialization();
        return dvd.getBigDecimal();
    }

    @Override
    public void setWidth(int desiredWidth,int desiredScale,boolean errorOnTrunc) throws StandardException{
        forceDeserialization();
        if(dvd instanceof VariableSizeDataValue){
            ((VariableSizeDataValue)dvd).setWidth(desiredWidth,desiredScale,errorOnTrunc);
        }else{
            throw new UnsupportedOperationException("Attempted to setWidth on wrapped "+dvd.getClass().getSimpleName()+" which does not implement VariableSizeDataValue");
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
