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
