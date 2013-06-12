package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.serial.DVDSerializer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.StringDataValue;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class LazyNumberDataValueDescriptor extends LazyDataValueDescriptor implements NumberDataValue{

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
        ndv.setValue(theValue.doubleValue());
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
        forceDeserialization();
        return ndv.cloneHolder();
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization) {
        resetForSerialization();
        return ndv.cloneValue(forceMaterialization);
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

        out.writeBoolean(deserialized);
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
        deserialized = in.readBoolean();

        init(extNDV, extSerializer);
    }

}
