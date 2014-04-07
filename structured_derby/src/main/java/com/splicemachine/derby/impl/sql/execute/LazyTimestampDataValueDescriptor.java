package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.serial.DVDSerializer;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DateTimeDataValue;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 2/12/14
 * Time: 11:12 AM
 * To change this template use File | Settings | File Templates.
 */
public class LazyTimestampDataValueDescriptor extends LazyDataValueDescriptor implements DateTimeDataValue
{

    private static Logger LOG = Logger.getLogger(LazyTimestampDataValueDescriptor.class);
    protected DateTimeDataValue dtdv;

    // Only for Kry to construct a LazyTimestampDataValueDescriptor instance
    public LazyTimestampDataValueDescriptor() {}

    public LazyTimestampDataValueDescriptor(DateTimeDataValue sdv){
        init(sdv);
    }

    /**
     * Initializes the Lazy String DVD, needs to call super to make sure the dvd on
     * the parent is set properly.
     *
		 * @param dtdv
		 *
		 */
    protected void init(DateTimeDataValue dtdv){
        super.init(dtdv);
        this.dtdv = dtdv;
    }

    @Override
    public DataValueDescriptor getNewNull() {
        return new LazyTimestampDataValueDescriptor((DateTimeDataValue) dtdv.getNewNull());
    }

    protected void forceDeserialization()  {
        if( !isDeserialized() && isSerialized()){
            super.forceDeserialization();
            this.dtdv = (DateTimeDataValue)this.dvd;
        }
    }
    @Override
    public NumberDataValue getYear(NumberDataValue result)
            throws StandardException {
        forceDeserialization();
        return dtdv.getYear(result);
    }

    @Override
    public NumberDataValue getMonth(NumberDataValue result)
            throws StandardException {
        forceDeserialization();
        return dtdv.getMonth(result);
    }

    @Override
    public NumberDataValue getDate(NumberDataValue result)
            throws StandardException {
        forceDeserialization();
        return dtdv.getDate(result);
    }

    @Override
    public NumberDataValue getHours(NumberDataValue result)
            throws StandardException {
        forceDeserialization();
        return dtdv.getHours(result);
    }

    @Override
    public NumberDataValue getMinutes(NumberDataValue result)
            throws StandardException {
        forceDeserialization();
        return dtdv.getMinutes(result);
    }

    @Override
    public NumberDataValue getSeconds(NumberDataValue result)
            throws StandardException {
        forceDeserialization();
        return dtdv.getSeconds(result);
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization) {
        forceDeserialization();
        DateTimeDataValue v = (DateTimeDataValue) dtdv.cloneValue(forceMaterialization);
        return new LazyTimestampDataValueDescriptor(v);
    }

    @Override
    public DataValueDescriptor cloneHolder() {
        forceDeserialization();
        DateTimeDataValue v = (DateTimeDataValue) dtdv.cloneHolder();
        return new LazyTimestampDataValueDescriptor(v);
    }

    @Override
    public NumberDataValue timestampDiff( int intervalType,
                                   DateTimeDataValue time1,
                                   java.sql.Date currentDate,
                                   NumberDataValue resultHolder)
            throws StandardException {
        forceDeserialization();
        return dtdv.timestampDiff(intervalType, time1, currentDate, resultHolder);

    }

    @Override
    public DateTimeDataValue timestampAdd( int intervalType,
                                    NumberDataValue intervalCount,
                                    java.sql.Date currentDate,
                                    DateTimeDataValue resultHolder)
            throws StandardException {
        forceDeserialization();
        return dtdv.timestampAdd(intervalType, intervalCount, currentDate, resultHolder);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        super.readExternal(in);
        DVDSerializer extSerializer = LazyDataValueFactory.getDVDSerializer(typeFormatId);

        dtdv = (DateTimeDataValue)dvd;
        init(dtdv);
    }

    @Override
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
	
    @Override
    public BooleanDataValue lessOrEquals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
    	/*
    	if (left.getTypeFormatId() == right.getTypeFormatId()) {
    		if (left.isLazy()) {
    			if (!right.isLazy())
    				((LazyDataValueDescriptor)right).serializeIfNeeded(((LazyDataValueDescriptor)right).descendingOrder);
    		} else if (right.isLazy()) {
    	
    	}
    		return SQLBoolean.truthValue(left, right, )
    	} 		
    	*/
        return SQLBoolean.truthValue(left, right, left.compare(right) <= 0);
    }

    @Override
    public BooleanDataValue greaterOrEquals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return SQLBoolean.truthValue(left, right, left.compare(right) >= 0);
    }
	
}
