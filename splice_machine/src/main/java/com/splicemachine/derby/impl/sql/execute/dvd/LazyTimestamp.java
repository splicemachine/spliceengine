package com.splicemachine.derby.impl.sql.execute.dvd;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;

import java.io.IOException;
import java.io.ObjectInput;

public class LazyTimestamp extends LazyDataValueDescriptor implements DateTimeDataValue{
    protected DateTimeDataValue dtdv;

    // Only for Kry to construct a LazyTimestampDataValueDescriptor instance
    public LazyTimestamp(){
    }

    public LazyTimestamp(DateTimeDataValue sdv){
        init(sdv);
    }

    /**
     * Initializes the Lazy String DVD, needs to call super to make sure the dvd on
     * the parent is set properly.
     *
     * @param dtdv
     */
    protected void init(DateTimeDataValue dtdv){
        super.init(dtdv);
        this.dtdv=dtdv;
    }

    @Override
    public DataValueDescriptor getNewNull(){
        return new LazyTimestamp();
    }

    @Override
    protected DataValueDescriptor newDescriptor(){
        dtdv=new SQLTimestamp();
        return dtdv;
    }

    protected void forceDeserialization(){
        if(!isDeserialized() && isSerialized()){
            super.forceDeserialization();
            this.dtdv=(DateTimeDataValue)this.dvd;
        }
    }

    @Override
    public NumberDataValue getYear(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dtdv.getYear(result);
    }

    @Override
    public NumberDataValue getQuarter(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dtdv.getQuarter(result);
    }

    @Override
    public NumberDataValue getMonth(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dtdv.getMonth(result);
    }

    @Override
    public StringDataValue getMonthName(StringDataValue result) throws StandardException{
        forceDeserialization();
        return dtdv.getMonthName(result);
    }

    @Override
    public NumberDataValue getWeek(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dtdv.getWeek(result);
    }

    @Override
    public NumberDataValue getWeekDay(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dtdv.getWeekDay(result);
    }

    @Override
    public StringDataValue getWeekDayName(StringDataValue result) throws StandardException{
        forceDeserialization();
        return dtdv.getWeekDayName(result);
    }

    @Override
    public NumberDataValue getDate(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dtdv.getDate(result);
    }

    @Override
    public NumberDataValue getDayOfYear(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dtdv.getDayOfYear(result);
    }

    @Override
    public NumberDataValue getHours(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dtdv.getHours(result);
    }

    @Override
    public NumberDataValue getMinutes(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dtdv.getMinutes(result);
    }

    @Override
    public NumberDataValue getSeconds(NumberDataValue result) throws StandardException{
        forceDeserialization();
        return dtdv.getSeconds(result);
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization){
        if(isNull())
            return new LazyTimestamp();
        else if(this.isSerialized()){
            LazyDataValueDescriptor lsdv=new LazyTimestamp();
            lsdv.initForDeserialization(tableVersion,serializer,bytes,offset,length,descendingOrder);
            return lsdv;
        }else{
            if(dtdv==null)
                return new LazyTimestamp((DateTimeDataValue)newDescriptor());
            else{
                return new LazyTimestamp((DateTimeDataValue)dtdv.cloneValue(forceMaterialization));
            }
        }
    }

    @Override
    public DataValueDescriptor cloneHolder() {
        if(isNull())
            return new LazyTimestamp();
        else if(isDeserialized())
            return new LazyTimestamp(dtdv);
        else{
           /*
            * Return a shallow clone, so just point to the same bytes
            */
            LazyTimestamp lv = new LazyTimestamp();
            lv.initForDeserialization(tableVersion,serializer,bytes,offset,length,descendingOrder);
            return lv;
        }
    }

    @Override
    public NumberDataValue timestampDiff(int intervalType,
                                         DateTimeDataValue time1,
                                         java.sql.Date currentDate,
                                         NumberDataValue resultHolder) throws StandardException{
        forceDeserialization();
        return dtdv.timestampDiff(intervalType,time1,currentDate,resultHolder);

    }

    @Override
    public DateTimeDataValue timestampAdd(int intervalType,
                                          NumberDataValue intervalCount,
                                          java.sql.Date currentDate,
                                          DateTimeDataValue resultHolder) throws StandardException{
        forceDeserialization();
        DateTimeDataValue resultHolderObject=resultHolder;
        if(resultHolder!=null){
            // if we don't pass the wrapped SQLTimestamp, we'll get a CCE in SQLTimestamp#timestampAdd()
            if(resultHolder instanceof LazyTimestamp){
                resultHolder=(DateTimeDataValue)resultHolderObject.getObject();
            }
        }
        return dtdv.timestampAdd(intervalType,intervalCount,currentDate,resultHolder);
    }

    @Override
    public DateTimeDataValue plus(DateTimeDataValue leftOperand,NumberDataValue daysToAdd,DateTimeDataValue returnValue) throws StandardException{
        forceDeserialization();
        return dtdv.plus(leftOperand,daysToAdd,returnValue);
    }

    @Override
    public DateTimeDataValue minus(DateTimeDataValue leftOperand,NumberDataValue daysToAdd,DateTimeDataValue returnValue) throws StandardException{
        forceDeserialization();
        return dtdv.minus(leftOperand,daysToAdd,returnValue);
    }

    @Override
    public NumberDataValue minus(DateTimeDataValue leftOperand,DateTimeDataValue daysToAdd,NumberDataValue returnValue) throws StandardException{
        forceDeserialization();
        return dtdv.minus(leftOperand,daysToAdd,returnValue);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{

        super.readExternal(in);

        dtdv=(DateTimeDataValue)dvd;
        init(dtdv);
    }

    @Override
    public int getLength() throws StandardException{
        forceDeserialization();
        /*
         * this matches SQLTimestamp which returns 12 (hardcoded)
         * even when the dvd is null
         */
        if (dvd == null)
            return 12;
        return dvd.getLength();
    }

    @Override
    public String toString(){
        try{
            return getString();
        }catch(StandardException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isDoubleType(){
        return false;
    }

    @Override
    public BooleanDataValue lessOrEquals(DataValueDescriptor left,DataValueDescriptor right) throws StandardException{
        return SQLBoolean.truthValue(left,right,left.compare(right)<=0);
    }

    @Override
    public BooleanDataValue greaterOrEquals(DataValueDescriptor left,DataValueDescriptor right) throws StandardException{
        return SQLBoolean.truthValue(left,right,left.compare(right)>=0);
    }

    @Override
    public Format getFormat(){
        return Format.TIMESTAMP;
    }

    @Override
    public int getTypeFormatId(){
        return StoredFormatIds.SQL_TIMESTAMP_ID;
    }

    @Override
    public int typePrecedence(){
        return TypeId.TIMESTAMP_PRECEDENCE;
    }

}
