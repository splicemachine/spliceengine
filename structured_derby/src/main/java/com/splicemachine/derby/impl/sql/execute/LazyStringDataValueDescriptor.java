package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.serial.DVDSerializer;
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

    public LazyStringDataValueDescriptor(StringDataValue sdv, DVDSerializer DVDSerializer){
        this.sdv = sdv;
        this.DVDSerializer = DVDSerializer;

    }

    protected StringDataValue unwrap(StringDataValue sdv){
        StringDataValue unwrapped = null;

        if(sdv instanceof LazyStringDataValueDescriptor){
            LazyStringDataValueDescriptor ldvd = (LazyStringDataValueDescriptor) sdv;
            ldvd.forceDeserialization();
            unwrapped = ldvd.getDvd();
        }else{
            unwrapped = sdv;
        }

        return unwrapped;
    }


    @Override
    public StringDataValue concatenate(StringDataValue leftOperand, StringDataValue rightOperand, StringDataValue result) throws StandardException {
        forceDeserialization();
        return getDvd().concatenate(unwrap(leftOperand), unwrap(rightOperand), result);
    }

    @Override
    public BooleanDataValue like(DataValueDescriptor pattern) throws StandardException {
        forceDeserialization();
        return getDvd().like(unwrap(pattern));
    }

    @Override
    public BooleanDataValue like(DataValueDescriptor pattern, DataValueDescriptor escape) throws StandardException {
        forceDeserialization();
        return getDvd().like(unwrap(pattern), unwrap(escape));
    }

    @Override
    public StringDataValue ansiTrim(int trimType, StringDataValue trimChar, StringDataValue result) throws StandardException {
        forceDeserialization();
        return getDvd().ansiTrim(trimType, trimChar, result);
    }

    @Override
    public StringDataValue upper(StringDataValue result) throws StandardException {
        forceDeserialization();
        return getDvd().upper(unwrap(result));
    }

    @Override
    public StringDataValue lower(StringDataValue result) throws StandardException {
        forceDeserialization();
        return getDvd().lower(unwrap(result));
    }

    @Override
    public NumberDataValue locate(StringDataValue searchFrom, NumberDataValue start, NumberDataValue result) throws StandardException {
        forceDeserialization();
        return getDvd().locate(searchFrom, start, result);
    }

    @Override
    public char[] getCharArray() throws StandardException {
        forceDeserialization();
        char[] c = getDvd().getCharArray();
        return c;
    }

    @Override
    public StringDataValue getValue(RuleBasedCollator collatorForComparison) {
        forceDeserialization();
        return getDvd().getValue(collatorForComparison);
    }

    @Override
    public StreamHeaderGenerator getStreamHeaderGenerator() {
        forceDeserialization();
        return getDvd().getStreamHeaderGenerator();
    }

    @Override
    public void setStreamHeaderFormat(Boolean usePreTenFiveHdrFormat) {
        forceDeserialization();
        getDvd().setStreamHeaderFormat(usePreTenFiveHdrFormat);
    }

    @Override
    public CharacterStreamDescriptor getStreamWithDescriptor() throws StandardException {
        forceDeserialization();
        return getDvd().getStreamWithDescriptor();
    }

    @Override
    public NumberDataValue charLength(NumberDataValue result) throws StandardException {
        forceDeserialization();
        return getDvd().charLength(result);
    }

    @Override
    public ConcatableDataValue substring(NumberDataValue start, NumberDataValue length, ConcatableDataValue result, int maxLen) throws StandardException {
        forceDeserialization();
        return getDvd().substring(start, length, result, maxLen);
    }

    @Override
    public void setWidth(int desiredWidth, int desiredScale, boolean errorOnTrunc) throws StandardException {
        forceDeserialization();
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
        forceDeserialization();
        return new LazyStringDataValueDescriptor((StringDataValue) getDvd().cloneHolder(), DVDSerializer);
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization) {
        forceDeserialization();
        return new LazyStringDataValueDescriptor((StringDataValue)  getDvd().cloneValue(forceMaterialization), DVDSerializer);
    }

    @Override
    public DataValueDescriptor recycle() {
        return null;
    }

    @Override
    public DataValueDescriptor getNewNull() {
        return new LazyStringDataValueDescriptor((StringDataValue) getDvd().getNewNull(), DVDSerializer);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeBoolean(getDvd() != null);

        if(getDvd() != null){
            out.writeObject(getDvd());
        }

        out.writeBoolean(isSerialized());

        if(isSerialized()){
            out.writeObject(new FormatableBitSet(dvdBytes));
        }

        out.writeUTF(DVDSerializer.getClass().getCanonicalName());

        out.writeBoolean(deserialized);
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
            DVDSerializer = (DVDSerializer) Class.forName(in.readUTF()).newInstance();
        }catch(Exception e){
            throw new RuntimeException("Error deserializing serialization class", e);
        }

        deserialized = in.readBoolean();
    }

}
