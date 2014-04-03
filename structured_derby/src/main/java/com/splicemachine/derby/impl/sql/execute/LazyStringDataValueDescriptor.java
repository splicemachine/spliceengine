package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.serial.DVDSerializer;
import com.splicemachine.utils.ByteSlice;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.CharacterStreamDescriptor;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.types.*;
import org.apache.derby.iapi.util.StringUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.text.RuleBasedCollator;

public class LazyStringDataValueDescriptor extends LazyDataValueDescriptor implements StringDataValue{

    private static Logger LOG = Logger.getLogger(LazyStringDataValueDescriptor.class);

    StringDataValue sdv;

    public LazyStringDataValueDescriptor(){}

    public LazyStringDataValueDescriptor(StringDataValue sdv, DVDSerializer dvdSerializer){
        init(sdv, dvdSerializer);
    }

    /**
     * Initializes the Lazy String DVD, needs to call super to make sure the dvd on
     * the parent is set properly.
     *
     * @param sdv
     * @param dvdSerializer
     */
    protected void init(StringDataValue sdv, DVDSerializer dvdSerializer){
        super.init(sdv, dvdSerializer);
        this.sdv = sdv;
    }

    protected StringDataValue unwrap(StringDataValue sdv){
        StringDataValue unwrapped = null;

        if(sdv instanceof LazyStringDataValueDescriptor){
            LazyStringDataValueDescriptor ldvd = (LazyStringDataValueDescriptor) sdv;
            ldvd.forceDeserialization();
            unwrapped = ldvd.sdv;
        }else{
            unwrapped = sdv;
        }

        return unwrapped;
    }

    @Override
    public void normalize(DataTypeDescriptor dtd, DataValueDescriptor source) throws StandardException {

        if(source.isLazy() && source instanceof LazyStringDataValueDescriptor) {

            LazyStringDataValueDescriptor ldvd = (LazyStringDataValueDescriptor) source;
            int desiredWidth = dtd.getMaximumWidth();
            byte[] sourceBytes = ldvd.getBytes();
            byte[] result = sourceBytes;
            int sourceWidth = sourceBytes.length;

            if (sourceWidth > desiredWidth) {
                // normalize the byte array to the desired length
                for (int posn = desiredWidth; posn < sourceWidth; posn++)
                {
                    if (sourceBytes[posn]-2 != ' ')
                    {
                        throw StandardException.newException(
                                SQLState.LANG_STRING_TRUNCATION,
                                getTypeName(),
                                StringUtil.formatForPrint(new String(sourceBytes)),
                                String.valueOf(desiredWidth));
                    }
                }
                result = new byte[desiredWidth];
                for (int i = 0; i < desiredWidth; ++i) {
                    result[i] = sourceBytes[i];
                }
            }
						this.isNull = result == null;
						if(!isNull){
								if(bytes==null)
										bytes = new ByteSlice();
								bytes.set(result,0,result.length);
						}
        } else {
            dvd.normalize(dtd, source);
            resetForSerialization();
        }
    }

    @Override
    public StringDataValue concatenate(StringDataValue leftOperand, StringDataValue rightOperand, StringDataValue result) throws StandardException {
        forceDeserialization();
        return sdv.concatenate(unwrap(leftOperand), unwrap(rightOperand), result);
    }

    @Override
    public BooleanDataValue like(DataValueDescriptor pattern) throws StandardException {
        forceDeserialization();
        return sdv.like(unwrap(pattern));
    }

    @Override
    public BooleanDataValue like(DataValueDescriptor pattern, DataValueDescriptor escape) throws StandardException {
        forceDeserialization();
        return sdv.like(unwrap(pattern), unwrap(escape));
    }

    @Override
    public StringDataValue ansiTrim(int trimType, StringDataValue trimChar, StringDataValue result) throws StandardException {
        forceDeserialization();
        return sdv.ansiTrim(trimType, trimChar, result);
    }

    @Override
    public StringDataValue upper(StringDataValue result) throws StandardException {
        forceDeserialization();
        return sdv.upper(unwrap(result));
    }

    @Override
    public StringDataValue lower(StringDataValue result) throws StandardException {
        forceDeserialization();
        return sdv.lower(unwrap(result));
    }

    @Override
    public NumberDataValue locate(StringDataValue searchFrom, NumberDataValue start, NumberDataValue result) throws StandardException {
        forceDeserialization();
        return sdv.locate(searchFrom, start, result);
    }

    @Override
    public char[] getCharArray() throws StandardException {
        forceDeserialization();
        char[] c = sdv.getCharArray();
        return c;
    }

    @Override
    public StringDataValue getValue(RuleBasedCollator collatorForComparison) {
        forceDeserialization();
        return sdv.getValue(collatorForComparison);
    }

    @Override
    public StreamHeaderGenerator getStreamHeaderGenerator() {
        forceDeserialization();
        return sdv.getStreamHeaderGenerator();
    }

    @Override
    public void setStreamHeaderFormat(Boolean usePreTenFiveHdrFormat) {
        forceDeserialization();
        sdv.setStreamHeaderFormat(usePreTenFiveHdrFormat);
    }

    @Override
    public CharacterStreamDescriptor getStreamWithDescriptor() throws StandardException {
        forceDeserialization();
        return sdv.getStreamWithDescriptor();
    }

    @Override
    public NumberDataValue charLength(NumberDataValue result) throws StandardException {
        forceDeserialization();
        return sdv.charLength(result);
    }

    @Override
    public ConcatableDataValue substring(NumberDataValue start, NumberDataValue length, ConcatableDataValue result, int maxLen) throws StandardException {
        forceDeserialization();
        return sdv.substring(start, length, result, maxLen);
    }

    @Override
    public void setWidth(int desiredWidth, int desiredScale, boolean errorOnTrunc) throws StandardException {
        forceDeserialization();
        sdv.setWidth(desiredWidth, desiredScale, errorOnTrunc);
        resetForSerialization();
    }

    @Override
    public DataValueDescriptor cloneHolder() {
        forceDeserialization();
        return new LazyStringDataValueDescriptor((StringDataValue) sdv.cloneHolder(), dvdSerializer);
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization) {
    	if (this.isSerialized()) {
    		LazyStringDataValueDescriptor lsdv = new LazyStringDataValueDescriptor((StringDataValue) sdv.cloneHolder(), dvdSerializer);
    		lsdv.initForDeserialization(bytes.array(), bytes.offset(), bytes.length(), descendingOrder);
    		return lsdv;
    	}
    	forceDeserialization();
    	return new LazyStringDataValueDescriptor((StringDataValue)  sdv.cloneValue(forceMaterialization), dvdSerializer);
    }

    @Override
    public DataValueDescriptor recycle() {
        restoreToNull();
        return this;
    }

    @Override
    public DataValueDescriptor getNewNull() {
        return new LazyStringDataValueDescriptor((StringDataValue) sdv.getNewNull(), dvdSerializer);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        super.readExternal(in);
        DVDSerializer extSerializer = LazyDataValueFactory.getDVDSerializer(typeFormatId);

        sdv = (StringDataValue)dvd;
        init(sdv, extSerializer);
    }

    public String toString() {
        try {
            return getString();
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }

	@Override
	public boolean isDoubleType() {
		return false;
	}

}
