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

package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.util.GregorianCalendar;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;


/**
 * Created by dmustafin on 5/7/15.
 */
@Category(ArchitectureIndependent.class)
public class SQLDateTest {

    private static final int GREGORIAN_YEAR = 1582;
    private static final int MAX_SUPPORTED_YEAR = 9999;
    private static final int MIN_SUPPORTED_YEAR = 1;


    private SQLDate createSqlDate(int year, int month, int day) throws StandardException {
        GregorianCalendar c = new GregorianCalendar();
        c.clear();
        c.set(year, month - 1, day);
        return new SQLDate(new java.sql.Date(c.getTimeInMillis()));
    }


    private String getYear(int year) {
        String sYear = Integer.toString(year);
        while (sYear.length() < 4) {
            sYear = "0" + sYear;
        }
        return sYear;
    }

    @Test
    public void shouldConvertArrayOfDatesStr() throws StandardException {
        for (int i = MAX_SUPPORTED_YEAR; i >= MIN_SUPPORTED_YEAR; i--) {
            String str = getYear(i) + "-01-10";
            SQLDate d = new SQLDate(str, false, null);
            String res = d.toString();
            //System.out.println(str + "  ::  " + res + "  ::  " + (str.equals(res) ? "y" : "NNN"));
            assertEquals(res, str);
        }
    }


    @Test
    public void shouldConvertArrayOfDates() throws StandardException {
        for (int i = MAX_SUPPORTED_YEAR; i >= MIN_SUPPORTED_YEAR; i--) {
            if (i != GREGORIAN_YEAR) {  // Gregorian calendar was applied, some days were skipped in October
                // more info at http://www.timeanddate.com/calendar/gregorian-calendar.html
                String sYear = getYear(i);
                String str = sYear + "-10-13";
                SQLDate d = createSqlDate(i, 10, 13);
                String res = d.toString();

                // for easy debugging
                //System.out.println(str + "  ::  " + res + "  ::  " + (str.equals(res) ? "y" : "NNNNNNNNNNNNNNNNN"));
                //if (!str.equals(res)) {
                //    d = createSqlDate(i, 10, 13);
                //    res = d.toString();
                //}

                assertEquals(str, res);
            }
        }
    }


    @Test
    public void shouldNotFailOnOctober() throws StandardException {
        SQLDate d = createSqlDate(GREGORIAN_YEAR, 10, 13);
        String res = d.toString();
        assertEquals("1582-10-23", res); // 10 days difference
    }


    @Test
    public void shouldConvertOldDate() throws StandardException {
        // date before 1884
        SQLDate dt = createSqlDate(1798, 1, 1);
        assertEquals("1798-01-01", dt.toString());

        // date after 1884
        dt = createSqlDate(2015, 1, 1);
        assertEquals("2015-01-01", dt.toString());

        dt = createSqlDate(1979, 1, 10);
        assertEquals("1979-01-10", dt.toString());

        // very old date
        dt = createSqlDate(1001, 1, 1);
        assertEquals("1001-01-01", dt.toString());

        // Jesus date
        dt = createSqlDate(1, 1, 1);
        assertEquals("0001-01-01", dt.toString());
    }

}
