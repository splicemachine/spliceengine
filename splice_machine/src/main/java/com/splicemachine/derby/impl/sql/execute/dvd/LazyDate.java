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
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;

import java.io.IOException;
import java.io.ObjectInput;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;

public class LazyDate extends LazyDataValueDescriptor implements DateTimeDataValue {
    protected DateTimeDataValue dtdv = null;

    // Only for Kry to construct a LazyDateDataValueDescriptor instance
    public LazyDate() {
    }

    public LazyDate(DateTimeDataValue sdv) {
        init(sdv);
    }

    /**
     * Initializes the Lazy String DVD, needs to call super to make sure the dvd on
     * the parent is set properly.
     *
     * @param dtdv the underlying data value
     */
    protected void init(DateTimeDataValue dtdv) {
        super.init(dtdv);
        this.dtdv = dtdv;
    }

    @Override
    public DataValueDescriptor getNewNull() {
        return new LazyDate();
    }

    @Override
    protected DataValueDescriptor newDescriptor() {
        dtdv = new SQLDate();
        return dtdv;
    }

    @Override
    public NumberDataValue getYear(NumberDataValue result) throws StandardException {
        forceDeserialization();
        return dtdv.getYear(result);
    }

    @Override
    public NumberDataValue getQuarter(NumberDataValue result) throws StandardException {
        forceDeserialization();
        return dtdv.getQuarter(result);
    }

    @Override
    public NumberDataValue getMonth(NumberDataValue result) throws StandardException {
        forceDeserialization();
        return dtdv.getMonth(result);
    }

    @Override
    public StringDataValue getMonthName(StringDataValue result) throws StandardException {
        forceDeserialization();
        return dtdv.getMonthName(result);
    }

    @Override
    public NumberDataValue getWeek(NumberDataValue result) throws StandardException {
        forceDeserialization();
        return dtdv.getWeek(result);
    }

    @Override
    public NumberDataValue getWeekDay(NumberDataValue result) throws StandardException {
        forceDeserialization();
        return dtdv.getWeekDay(result);
    }

    @Override
    public StringDataValue getWeekDayName(StringDataValue result) throws StandardException {
        forceDeserialization();
        return dtdv.getWeekDayName(result);
    }

    @Override
    public NumberDataValue getDate(NumberDataValue result) throws StandardException {
        forceDeserialization();
        return dtdv.getDate(result);
    }

    @Override
    public NumberDataValue getDayOfYear(NumberDataValue result) throws StandardException {
        forceDeserialization();
        return dtdv.getDayOfYear(result);
    }

    @Override
    public NumberDataValue getHours(NumberDataValue result) throws StandardException {
        forceDeserialization();
        return dtdv.getHours(result);
    }

    @Override
    public NumberDataValue getMinutes(NumberDataValue result) throws StandardException {
        forceDeserialization();
        return dtdv.getMinutes(result);
    }

    @Override
    public NumberDataValue getSeconds(NumberDataValue result) throws StandardException {
        forceDeserialization();
        return dtdv.getSeconds(result);
    }

    @Override
    public  DataValueDescriptor cloneValue(boolean forceMaterialization){
        if(isNull())
            return new LazyDate();
        else if(this.isSerialized()){
            LazyDataValueDescriptor lsdv=new LazyDate();
            lsdv.initForDeserialization(tableVersion,serializer,bytes,offset,length,descendingOrder);
            return lsdv;
        }else{
            if(dtdv==null)
                return new LazyDate((DateTimeDataValue)newDescriptor());
            else{
                return new LazyDate((DateTimeDataValue)dtdv.cloneValue(forceMaterialization));
            }
        }
    }

    @Override
    public DataValueDescriptor cloneHolder() {
       if(isNull())
           return new LazyDate();
       else if(isDeserialized())
           return new LazyDate(dtdv);
       else{
           /*
            * Return a shallow clone, so just point to the same bytes
            */
           LazyDate lv = new LazyDate();
           lv.initForDeserialization(tableVersion,serializer,bytes,offset,length,descendingOrder);
           return lv;
       }
    }

    @Override
    public NumberDataValue timestampDiff(int intervalType,
                                         DateTimeDataValue time1,
                                         java.sql.Date currentDate,
                                         NumberDataValue resultHolder) throws StandardException {
        forceDeserialization();
        return dtdv.timestampDiff(intervalType, time1, currentDate, resultHolder);

    }

    @Override
    public DateTimeDataValue timestampAdd(int intervalType,
                                          NumberDataValue intervalCount,
                                          java.sql.Date currentDate,
                                          DateTimeDataValue resultHolder) throws StandardException {
        forceDeserialization();
        DateTimeDataValue resultHolderObject = resultHolder;
        if (resultHolder != null) {
            // if we don't pass the wrapped SQLDate, we'll get a CCE in SQLDate#timestampAdd()
            if (resultHolder instanceof LazyDate) {
                resultHolder = (DateTimeDataValue) resultHolderObject.getObject();
            }
        }
        return dtdv.timestampAdd(intervalType, intervalCount, currentDate, resultHolder);
    }

    @Override
    public DateTimeDataValue plus(DateTimeDataValue leftOperand, NumberDataValue daysToAdd, DateTimeDataValue returnValue) throws StandardException {
        forceDeserialization();
        return dtdv.plus(leftOperand, daysToAdd, returnValue);
    }

    @Override
    public DateTimeDataValue minus(DateTimeDataValue leftOperand, NumberDataValue daysToAdd, DateTimeDataValue returnValue) throws StandardException {
        forceDeserialization();
        return dtdv.minus(leftOperand, daysToAdd, returnValue);
    }

    @Override
    public NumberDataValue minus(DateTimeDataValue leftOperand, DateTimeDataValue daysToAdd, NumberDataValue returnValue) throws StandardException {
        forceDeserialization();
        return dtdv.minus(leftOperand, daysToAdd, returnValue);
    }

    @Override
    public void setValue(String value,Calendar cal) throws StandardException{
        if(dtdv==null) createNewDescriptor();
        dtdv.setValue(value,cal);
        resetForSerialization();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        super.readExternal(in);

        dtdv = (DateTimeDataValue) dvd;
        init(dtdv);
    }

    @Override
    public int getLength() throws StandardException{
        forceDeserialization();
        /*
         * this matches SQLDate which returns 4 (hardcoded)
         * even when the dvd is null
         */
        if (dvd == null)
            return 4;
        return dvd.getLength();
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
        return SQLBoolean.truthValue(left, right, left.compare(right) <= 0);
    }

    @Override
    public BooleanDataValue greaterOrEquals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return SQLBoolean.truthValue(left, right, left.compare(right) >= 0);
    }

    @Override
    public Format getFormat() {
        return Format.DATE;
    }

    @Override
    public int getTypeFormatId() {
        return StoredFormatIds.SQL_DATE_ID;
    }

    @Override
    public int typePrecedence() {
        return TypeId.DATE_PRECEDENCE;
    }

    @Override
    public Date getDate(java.util.Calendar cal) throws StandardException {
        forceDeserialization();
        return dvd.getDate(cal);
    }

    @Override
    public Timestamp getTimestamp(java.util.Calendar cal) throws StandardException {
        forceDeserialization();
        return dvd.getTimestamp(cal);
    }

    @Override
    public String getString() throws StandardException {
        forceDeserialization();
        return dvd.getString();
    }

    @Override
    public Object getObject() throws StandardException {
        forceDeserialization();
        return dvd.getObject();
    }

    @Override
    public void decodeFromKey(PositionedByteRange builder) throws StandardException {
        forceDeserialization();
        dvd.decodeFromKey(builder);
    }

    @Override
    public void encodeIntoKey(PositionedByteRange builder, Order order) throws StandardException {
        forceDeserialization();
        dvd.encodeIntoKey(builder,order);
    }

    @Override
    public int encodedKeyLength() throws StandardException {
        forceDeserialization();
        return dvd.encodedKeyLength();
    }

    @Override
    public void read(UnsafeRow unsafeRow, int ordinal) throws StandardException {
        forceDeserialization();
        dvd.read(unsafeRow, ordinal);
    }

    @Override
    public void write(UnsafeRowWriter unsafeRowWriter, int ordinal) throws StandardException {
        forceDeserialization();
        dvd.write(unsafeRowWriter, ordinal);
    }
}
