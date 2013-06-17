package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.serial.DVDSerializer;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.VariableSizeDataValue;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class LazyNumberDataValueDescriptor extends LazyDataValueDescriptor implements NumberDataValue, VariableSizeDataValue{

    NumberDataValue ndv;

    public LazyNumberDataValueDescriptor(){}

    public LazyNumberDataValueDescriptor(NumberDataValue ndv, DVDSerializer dvdSerializer){
        init(ndv, dvdSerializer);
    }

    public void init(NumberDataValue ndv, DVDSerializer dvdSerializer){
        super.init(ndv, dvdSerializer);
        this.ndv = ndv;
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
        LazyNumberDataValueDescriptor newDvd = new LazyNumberDataValueDescriptor((NumberDataValue) ndv.cloneHolder(), dvdSerializer);
        newDvd.dvdBytes = this.dvdBytes;
        newDvd.deserialized = this.deserialized;
        newDvd.updateNullFlag();

        return newDvd;
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization) {

        LazyNumberDataValueDescriptor newDvd = new LazyNumberDataValueDescriptor((NumberDataValue) ndv.cloneValue(forceMaterialization), dvdSerializer);
        newDvd.dvdBytes = this.dvdBytes;
        newDvd.deserialized = this.deserialized;
        newDvd.updateNullFlag();

        return newDvd;
    }

    @Override
    public DataValueDescriptor getNewNull() {
        return ndv.getNewNull();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeBoolean(ndv != null);

        if(ndv != null){
            out.writeUTF(ndv.getClass().getCanonicalName());
        }

        byte[] bytes = null;

        try{
            bytes = getBytes();
        }catch(StandardException e){
            throw new IOException("Error reading bytes from DVD", e);
        }

        out.writeBoolean(bytes != null);

        if(bytes != null){
            out.writeObject(new FormatableBitSet(bytes));
        }

        out.writeUTF(dvdSerializer.getClass().getCanonicalName());

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        NumberDataValue extNDV = null;

        if(in.readBoolean()){
            extNDV = (NumberDataValue) createClassInstance(in.readUTF());
            extNDV.setToNull();
        }

        if(in.readBoolean()){
            FormatableBitSet fbs = (FormatableBitSet) in.readObject();
            dvdBytes = fbs.getByteArray();
        }

        DVDSerializer extSerializer = (DVDSerializer) createClassInstance(in.readUTF());
        deserialized = false;

        init(extNDV, extSerializer);
    }

    @Override
    public void setWidth(int desiredWidth, int desiredScale, boolean errorOnTrunc) throws StandardException {
        if(ndv instanceof VariableSizeDataValue){
            ((VariableSizeDataValue) ndv).setWidth(desiredWidth, desiredScale, errorOnTrunc);
        }else{
            throw new UnsupportedOperationException("Attempted to setWidth on wrapped " + ndv.getClass().getSimpleName() + " which does not implement VariableSizeDataValue" );
        }
    }
}
