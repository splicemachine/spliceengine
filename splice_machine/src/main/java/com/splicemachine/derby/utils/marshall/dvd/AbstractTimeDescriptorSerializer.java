/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.db.iapi.types.DataValueDescriptor;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * @author Scott Fines
 * Date: 4/2/14
 */
public abstract class AbstractTimeDescriptorSerializer implements TimeValuedSerializer {
    // Must isolate Calendar objects to a single thread here b/c Serializer objects are
    // evidently shared; this fixes obscure cluster-only bug
    private ThreadLocal<Calendar> calendar = initCal();

    @Override
    public void setCalendar(Calendar calendar) {
        if (this.calendar == null) {
            // could have been closed (these are closables)
            this.calendar = initCal();
        }
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

    public static abstract class Factory implements DescriptorSerializer.Factory {
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

    private ThreadLocal<Calendar> initCal() {
        return new ThreadLocal<Calendar>() {
            @Override
            protected Calendar initialValue() {
                return new GregorianCalendar();
            }
        };
    }
}
