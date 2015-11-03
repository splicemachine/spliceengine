package com.splicemachine.derby.impl.sql.execute.dvd;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl;
import com.splicemachine.db.iapi.types.NumberDataValue;
import com.splicemachine.db.iapi.types.VariableSizeDataValue;

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
}
