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

public abstract class LazyNumberDataValueDescriptor extends LazyDataValueDescriptor implements NumberDataValue, VariableSizeDataValue{

    NumberDataValue ndv;

    public LazyNumberDataValueDescriptor(){
    }

    public LazyNumberDataValueDescriptor(NumberDataValue ndv){
        init(ndv);
    }

    public void init(NumberDataValue ndv){
        super.init(ndv);
        this.ndv=ndv;
    }

    @Override
    public DataValueFactoryImpl.Format getFormat(){
        if(ndv==null){
            dvd=ndv=(NumberDataValue)newDescriptor();
        }
        return ndv.getFormat();
    }

    public NumberDataValue plus(NumberDataValue addend1,NumberDataValue addend2,NumberDataValue result) throws StandardException{
        forceDeserialization();
        return ndv.plus(addend1,addend2,result);
    }

    @Override
    public NumberDataValue minus(NumberDataValue left,NumberDataValue right,NumberDataValue result) throws StandardException{
        forceDeserialization();
        return ndv.minus(left,right,result);
    }

    @Override
    public NumberDataValue times(NumberDataValue left,NumberDataValue right,NumberDataValue result) throws StandardException{
        forceDeserialization();
        return ndv.times(left,right,result);
    }

    @Override
    public NumberDataValue divide(NumberDataValue dividend,NumberDataValue divisor,NumberDataValue result) throws StandardException{
        forceDeserialization();
        return ndv.divide(dividend,divisor,result);
    }

    @Override
    public NumberDataValue divide(NumberDataValue dividend,NumberDataValue divisor,NumberDataValue result,int scale) throws StandardException{
        forceDeserialization();
        return ndv.divide(dividend,divisor,result);
    }

    @Override
    public NumberDataValue mod(NumberDataValue dividend,NumberDataValue divisor,NumberDataValue result) throws StandardException{
        forceDeserialization();
        return ndv.mod(dividend,divisor,result);
    }

    @Override
    public NumberDataValue minus(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return ndv.minus(result);
    }

    @Override
    public NumberDataValue absolute(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return ndv.absolute(result);
    }

    @Override
    public NumberDataValue sqrt(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return ndv.sqrt(result);
    }

    @Override
    public void setValue(Number theValue) throws StandardException{
        if(ndv==null)
            dvd = ndv = (NumberDataValue)newDescriptor();
        ndv.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public int getDecimalValuePrecision(){
        forceDeserialization();
        return ndv.getDecimalValuePrecision();
    }

    @Override
    public int getDecimalValueScale(){
        forceDeserialization();
        return ndv.getDecimalValueScale();
    }

    @Override
    public BigDecimal getBigDecimal() throws StandardException{
        forceDeserialization();
        return ndv.getBigDecimal();
    }

    @Override
    public void setWidth(int desiredWidth,int desiredScale,boolean errorOnTrunc) throws StandardException{
        if(ndv==null)
            dvd = ndv = (NumberDataValue)newDescriptor();
        if(ndv instanceof VariableSizeDataValue){
            ((VariableSizeDataValue)ndv).setWidth(desiredWidth,desiredScale,errorOnTrunc);
        }else{
            throw new UnsupportedOperationException("Attempted to setWidth on wrapped "+ndv.getClass().getSimpleName()+" which does not implement VariableSizeDataValue");
        }
    }

    @Override
    public void decodeFromKey(PositionedByteRange builder) throws StandardException {
        forceDeserialization();
        ndv.decodeFromKey(builder);
    }

    @Override
    public void encodeIntoKey(PositionedByteRange builder, Order order) throws StandardException {
        forceDeserialization();
        ndv.encodeIntoKey(builder,order);
    }

    @Override
    public int encodedKeyLength() throws StandardException {
        forceDeserialization();
        return ndv.encodedKeyLength();
    }

    @Override
    public void read(UnsafeRow unsafeRow, int ordinal) throws StandardException {
        forceDeserialization();
        ndv.read(unsafeRow, ordinal);
    }

    @Override
    public void write(UnsafeRowWriter unsafeRowWriter, int ordinal) throws StandardException {
        forceDeserialization();
        ndv.write(unsafeRowWriter, ordinal);
    }

}
