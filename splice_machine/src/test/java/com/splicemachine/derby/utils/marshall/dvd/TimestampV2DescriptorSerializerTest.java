/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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


import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Timestamp;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;


/**
 * Created by dmustafin on 5/13/15.
 */
@Category(ArchitectureIndependent.class)
public class TimestampV2DescriptorSerializerTest {

    private static final long NANOS_IN_HOUR = 1000 * 1000 * 1000 * 60 * 60;

    @Test
    public void shouldSerialize() throws StandardException {
        // here we should take into account current time-zone's offset
        // i.e. CDT Mon 19:00 ; London Tue 00:00 ; Odessa Tue 02:00 ; Moscow Tue 03:00
        // all timestamp in Splice are in CDT (GMT-5) and nano-seconds (x * 10^9)
        final long CDT_OFFSET_HOURS = -6;
        final long CDT_OFFSET_MIN   = CDT_OFFSET_HOURS * 60;
        final long CDT_OFFSET_SEC   = CDT_OFFSET_MIN   * 60;
        final long CDT_OFFSET_MILLI = CDT_OFFSET_SEC   * 1000;
        final long CDT_OFFSET_MICRO = CDT_OFFSET_MILLI * 1000;
        final long CDT_OFFSET_NANO  = CDT_OFFSET_MICRO * 1000;

        TimeZone timeZone = TimeZone.getDefault();
        long currentOffsetMilli = timeZone.getRawOffset(); // do not change this here because of int arithmetic overflow!
        long currentOffsetNano = currentOffsetMilli * 1000 * 1000;
//        System.out.println("     curent offset = " + (currentOffsetMilli / 1000 / 3600) + " h");

        long deltaOffset = currentOffsetNano - CDT_OFFSET_NANO;
//        System.out.println("   offset over CDT = " + deltaOffset + " = " + (deltaOffset / NANOS_IN_HOUR) + " h");
//        System.out.println();

        testTimestamps(2000,   950162400000000000L,  deltaOffset);
        testTimestamps(1678, -9211082400000000000L,  deltaOffset);
        testTimestamps(2262,  9218124000000000000L,  deltaOffset);
    }





    @Test
    public void shouldFailOnSmallDate() {
        String errorCode = "N/A";
        try {
            TimestampV2DescriptorSerializer.formatLong(getTimestamp(1677));
        } catch (StandardException e) {
            errorCode = e.getSqlState();
        }

        assertEquals(SQLState.LANG_DATE_TIME_ARITHMETIC_OVERFLOW, errorCode);
    }


    @Test
    public void shouldFailOnLargeDate() {
        String errorCode = "N/A";
        try {
            TimestampV2DescriptorSerializer.formatLong(getTimestamp(2263));
        } catch (StandardException e) {
            errorCode = e.getSqlState();
        }

        assertEquals(SQLState.LANG_DATE_TIME_ARITHMETIC_OVERFLOW, errorCode);
    }


    private Timestamp getTimestamp(int year) {
        GregorianCalendar cal = new GregorianCalendar(year, 01, 10);

        return new Timestamp(cal.getTimeInMillis());
    }


    private void testTimestamps(int year, long expectedTimestamp, long deltaOffset) throws StandardException {
        //System.out.println("expected timestamp = " + expectedTimestamp);
        //System.out.println("              year = " + year);

        long ts = TimestampV2DescriptorSerializer.formatLong(getTimestamp(year));
        //System.out.println("            result = " + ts);
        ts = ts + deltaOffset;
        //System.out.println("    shifted result = " + ts);

        long d = expectedTimestamp - ts;
        //System.out.println("             delta = " + d + "   h = " + (d / NANOS_IN_HOUR));

        assertEquals(expectedTimestamp, ts);

        //System.out.println();
    }
}
