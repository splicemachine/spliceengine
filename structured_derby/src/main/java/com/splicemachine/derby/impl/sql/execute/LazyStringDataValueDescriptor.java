package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.serial.SerializerThunk;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.CharacterStreamDescriptor;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.types.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.text.RuleBasedCollator;

public class LazyStringDataValueDescriptor extends LazyDataValueDescriptor implements StringDataValue{

    private static Logger LOG = Logger.getLogger(LazyStringDataValueDescriptor.class);

    private StringDataValue sdv;

    public LazyStringDataValueDescriptor(){}

    public LazyStringDataValueDescriptor(StringDataValue sdv, SerializerThunk serializerThunk){
        this.sdv = sdv;
        this.serializerThunk = serializerThunk;

    }

    protected StringDataValue unwrap(StringDataValue sdv){
        StringDataValue unwrapped = null;

        if(sdv instanceof LazyStringDataValueDescriptor){
            LazyStringDataValueDescriptor ldvd = (LazyStringDataValueDescriptor) sdv;
            unwrapped = ldvd.getDvd();
        }else{
            unwrapped = sdv;
        }

        return unwrapped;
    }


    @Override
    public StringDataValue concatenate(StringDataValue leftOperand, StringDataValue rightOperand, StringDataValue result) throws StandardException {
        return getDvd().concatenate(unwrap(leftOperand), unwrap(rightOperand), result);
    }

    @Override
    public BooleanDataValue like(DataValueDescriptor pattern) throws StandardException {
        return getDvd().like(unwrap(pattern));
    }

    @Override
    public BooleanDataValue like(DataValueDescriptor pattern, DataValueDescriptor escape) throws StandardException {
        return getDvd().like(unwrap(pattern), unwrap(escape));
    }

    @Override
    public StringDataValue ansiTrim(int trimType, StringDataValue trimChar, StringDataValue result) throws StandardException {
        return null;
    }

    @Override
    public StringDataValue upper(StringDataValue result) throws StandardException {
        return getDvd().upper(unwrap(result));
    }

    @Override
    public StringDataValue lower(StringDataValue result) throws StandardException {
        return getDvd().upper(unwrap(result));
    }

    @Override
    public NumberDataValue locate(StringDataValue searchFrom, NumberDataValue start, NumberDataValue result) throws StandardException {
        return getDvd().locate(searchFrom, start, result);
    }

    @Override
    public char[] getCharArray() throws StandardException {
        char[] c = getDvd().getCharArray();
        resetForSerialization();
        return c;
    }

    @Override
    public StringDataValue getValue(RuleBasedCollator collatorForComparison) {
        return getDvd().getValue(collatorForComparison);
    }

    @Override
    public StreamHeaderGenerator getStreamHeaderGenerator() {
        return getDvd().getStreamHeaderGenerator();
    }

    @Override
    public void setStreamHeaderFormat(Boolean usePreTenFiveHdrFormat) {
        getDvd().setStreamHeaderFormat(usePreTenFiveHdrFormat);
    }

    @Override
    public CharacterStreamDescriptor getStreamWithDescriptor() throws StandardException {
        return getDvd().getStreamWithDescriptor();
    }

    @Override
    public NumberDataValue charLength(NumberDataValue result) throws StandardException {
        return getDvd().charLength(result);
    }

    @Override
    public ConcatableDataValue substring(NumberDataValue start, NumberDataValue length, ConcatableDataValue result, int maxLen) throws StandardException {
        return getDvd().substring(start, length, result, maxLen);
    }

    @Override
    public void setWidth(int desiredWidth, int desiredScale, boolean errorOnTrunc) throws StandardException {
        getDvd().setWidth(desiredWidth, desiredScale, errorOnTrunc);
        resetForSerialization();
    }

    @Override
    protected StringDataValue getDvd() {
        return sdv;
    }

    private void setDvd(StringDataValue sdv) {
        this.sdv = sdv;
    }

    @Override
    public DataValueDescriptor cloneHolder() {
        return new LazyStringDataValueDescriptor((StringDataValue) getDvd().cloneHolder(), serializerThunk);
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization) {
        return new LazyStringDataValueDescriptor((StringDataValue)  getDvd().cloneValue(forceMaterialization), serializerThunk);
    }

    @Override
    public DataValueDescriptor recycle() {
        return null;
    }

    @Override
    public DataValueDescriptor getNewNull() {
        return new LazyStringDataValueDescriptor((StringDataValue) getDvd().getNewNull(), serializerThunk);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeBoolean(getDvd() != null);

        if(getDvd() != null){
            out.writeObject(getDvd());
        }

        out.writeBoolean(dvdBytes != null);

        if(dvdBytes != null){
            out.writeObject(new FormatableBitSet(dvdBytes));
        }

        out.writeUTF(serializerThunk.getClass().getCanonicalName());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        if(in.readBoolean()){
            setDvd((StringDataValue) in.readObject());
        }

        if(in.readBoolean()){
            FormatableBitSet fbs = (FormatableBitSet) in.readObject();
            dvdBytes = fbs.getByteArray();
        }

        try{
            serializerThunk = (SerializerThunk) Class.forName(in.readUTF()).newInstance();
        }catch(Exception e){
            throw new RuntimeException("Error deserializing serialization class", e);
        }
    }

}
