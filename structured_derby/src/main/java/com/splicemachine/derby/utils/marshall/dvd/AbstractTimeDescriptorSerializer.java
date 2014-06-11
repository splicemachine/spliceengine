package com.splicemachine.derby.utils.marshall.dvd;

import org.apache.derby.iapi.types.DataValueDescriptor;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * @author Scott Fines
 * Date: 4/2/14
 */
public abstract class AbstractTimeDescriptorSerializer implements TimeValuedSerializer {
    // Must isolate Calendar objects to a single thread here b/c Serializer objects are
    // evidently shared; this fixes obscure cluster-only bug
    private ThreadLocal<Calendar> calendar = new ThreadLocal<Calendar>() {
        @Override
        protected Calendar initialValue() {
            return new GregorianCalendar();
        }
    };

    @Override
    public void setCalendar(Calendar calendar) {
        this.calendar.set(calendar);
    }

    protected Calendar getCalendar() {
        return calendar.get();
    }

    @Override
    public boolean isScalarType() { return true; }

    @Override
    public boolean isFloatType() { return false; }

    @Override
    public boolean isDoubleType() { return false; }

    @Override
    public void close() { calendar = null; }

    protected static abstract class Factory implements DescriptorSerializer.Factory {
        @Override
        public boolean applies(DataValueDescriptor dvd) {
            return dvd != null && applies(dvd.getTypeFormatId());
        }

        @Override
        public boolean isScalar() { return true; }

        @Override
        public boolean isFloat() { return false; }

        @Override
        public boolean isDouble() { return false; }
    }
}
