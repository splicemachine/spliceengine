package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.utils.ByteSlice;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactoryImpl;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.VariableSizeDataValue;

import java.io.IOException;
import java.io.ObjectInput;

public class LazyNumberDataValueDescriptor extends LazyDataValueDescriptor implements NumberDataValue, VariableSizeDataValue{

    NumberDataValue ndv;

    public LazyNumberDataValueDescriptor(){}

    public LazyNumberDataValueDescriptor(NumberDataValue ndv){
        init(ndv);
    }

    public void init(NumberDataValue ndv){
        super.init(ndv);
        this.ndv = ndv;
    }

		@Override
		public DataValueFactoryImpl.Format getFormat() {
				return ndv.getFormat();
		}

		@Override
		public boolean isDoubleType() {
				return ndv.isDoubleType();
		}

		@Override
    public NumberDataValue plus(NumberDataValue addend1, NumberDataValue addend2, NumberDataValue result) throws StandardException {
        forceDeserialization();
        return ndv.plus(addend1, addend2, result);
    }

    @Override
    public NumberDataValue minus(NumberDataValue left, NumberDataValue right, NumberDataValue result) throws StandardException {
        forceDeserialization();
        return ndv.minus(left, right, result);
    }

    @Override
    public NumberDataValue times(NumberDataValue left, NumberDataValue right, NumberDataValue result) throws StandardException {
        forceDeserialization();
        return ndv.times(left, right, result);
    }

    @Override
    public NumberDataValue divide(NumberDataValue dividend, NumberDataValue divisor, NumberDataValue result) throws StandardException {
        forceDeserialization();
        return ndv.divide(dividend, divisor, result);
    }

    @Override
    public NumberDataValue divide(NumberDataValue dividend, NumberDataValue divisor, NumberDataValue result, int scale) throws StandardException {
        forceDeserialization();
        return ndv.divide(dividend, divisor, result);
    }

    @Override
    public NumberDataValue mod(NumberDataValue dividend, NumberDataValue divisor, NumberDataValue result) throws StandardException {
        forceDeserialization();
        return ndv.mod(dividend, divisor, result);
    }

    @Override
    public NumberDataValue minus(NumberDataValue result) throws StandardException {
        forceDeserialization();
        return ndv.minus(result);
    }

    @Override
    public NumberDataValue absolute(NumberDataValue result) throws StandardException {
        forceDeserialization();
        return ndv.absolute(result);
    }

    @Override
    public NumberDataValue sqrt(NumberDataValue result) throws StandardException {
        forceDeserialization();
        return ndv.sqrt(result);
    }

    @Override
    public void setValue(Number theValue) throws StandardException {
        ndv.setValue(theValue);
        resetForSerialization();
    }

    @Override
    public int getDecimalValuePrecision() {
        forceDeserialization();
        return ndv.getDecimalValuePrecision();
    }

    @Override
    public int getDecimalValueScale() {
        forceDeserialization();
        return ndv.getDecimalValueScale();
    }

    @Override
    public DataValueDescriptor cloneHolder() {
        LazyNumberDataValueDescriptor newDvd = new LazyNumberDataValueDescriptor((NumberDataValue) ndv.cloneHolder());
				newDvd.serializer = serializer;
				newDvd.tableVersion = tableVersion;
        newDvd.bytes = new ByteSlice(this.bytes);
        newDvd.deserialized = this.deserialized;
        newDvd.updateNullFlag();

        return newDvd;
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization) {

        LazyNumberDataValueDescriptor newDvd = new LazyNumberDataValueDescriptor((NumberDataValue) ndv.cloneValue(forceMaterialization));
				newDvd.serializer = serializer;
				newDvd.tableVersion = tableVersion;
        newDvd.bytes = new ByteSlice(this.bytes);
        newDvd.deserialized = this.deserialized;
        newDvd.updateNullFlag();

        return newDvd;
    }

    @Override
    public DataValueDescriptor getNewNull() {
        return ndv.getNewNull();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        ndv = (NumberDataValue)dvd;
        init(ndv);
    }

    @Override
    public void setWidth(int desiredWidth, int desiredScale, boolean errorOnTrunc) throws StandardException {
        if(ndv instanceof VariableSizeDataValue){
            ((VariableSizeDataValue) ndv).setWidth(desiredWidth, desiredScale, errorOnTrunc);
        }else{
            throw new UnsupportedOperationException("Attempted to setWidth on wrapped " + ndv.getClass().getSimpleName() + " which does not implement VariableSizeDataValue" );
        }
    }

    public String toString() {
        try {
            return getString();
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }
}
