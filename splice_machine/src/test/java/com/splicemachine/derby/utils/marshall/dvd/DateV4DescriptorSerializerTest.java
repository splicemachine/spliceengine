/*
 * Copyright (c) 2018 Splice Machine, Inc.
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


import com.splicemachine.db.iapi.db.DatabaseContext;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.i18n.LocaleFinder;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLDate;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.ZoneId;
import java.sql.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;


/**
 * Created by msirek on 9/13/18.
 */
@Category(ArchitectureIndependent.class)
public class DateV4DescriptorSerializerTest {


    String dates[] = {"2020-01-01","2035-01-01","2004-01-01","2000-01-01","2006-02-28","2012-10-28",
                      "2037-04-01","2038-12-11","2040-04-30","2018-01-01","2022-01-01","2019-05-31",
                      "1932-03-31","1582-10-04", "1984-02-29","9999-12-31","0001-01-01","1111-02-03",
                      "2222-03-04","4444-04-05","1776-07-04","1196-05-25","1741-11-10", "1847-08-09",
                      "1850-04-18", "1879-02-26", "2061-11-30", "1999-12-31", "8888-08-08"};

    private void testSingleDate(String dateString, LocaleFinder localeFinder, ZoneId zoneId,
                                DescriptorSerializer descriptorSerializer) throws StandardException {
        byte[] bytes;
        GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone(zoneId));
        DataValueDescriptor dvdSource = new SQLDate(dateString, false, localeFinder, cal);
        DataValueDescriptor dvdTarget = new SQLDate((Date)null);
        bytes = Encoding.encode(((SQLDate) dvdSource).getDiskEncodedDate(), false);
        descriptorSerializer.decodeDirect(dvdTarget, bytes, 0, bytes.length, false);
        assertEquals(dvdSource, dvdTarget);
    }

    @Test
    public void testDates() throws StandardException {

        DatabaseContext dc = (DatabaseContext) ContextService.getContext(DatabaseContext.CONTEXT_ID);
        LocaleFinder localeFinder = (dc == null) ? null : dc.getDatabase();
        DescriptorSerializer dateDescriptorSerializer = DateV4DescriptorSerializer.INSTANCE_FACTORY.newInstance();

        try {
            // Encoding/decoding of dates should not be timezone-sensitive.
            for (String zId : ZoneId.getAvailableZoneIds()) {
                ZoneId zoneId = ZoneId.of(zId);
                for (String date : dates)
                    testSingleDate(date, localeFinder, zoneId, dateDescriptorSerializer);
            }
        } catch (Exception e) {
            Assert.fail("Exception while testing Date encoding/decoding.");
        }
    }
}
