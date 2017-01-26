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

package com.splicemachine.derby.utils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.monitor.ModuleFactory;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.J2SEDataValueFactory;
import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;

import com.splicemachine.db.iapi.types.SQLDate;
import org.junit.experimental.categories.Category;

/**
* Created by yifu on 6/24/14.
*/
@Category(ArchitectureIndependent.class)
public class setFromTest {
    protected static J2SEDataValueFactory dvf = new J2SEDataValueFactory();

    @BeforeClass
    public static void startup() throws StandardException {
        ModuleFactory monitor = Monitor.getMonitorLite();
        Monitor.setMonitor(monitor);
        monitor.setLocale(new Properties(), Locale.getDefault().toString());
        dvf.boot(false, new Properties());
    }
    /*
    As the setFrom method is set as protected in derby, we here use the setValue public method which is what setFrom
     really invokes in its execution
     */
    @Test
    public void testSetFromMethod()throws Exception{
        Timestamp ts;
        DataValueDescriptor des;
        SQLDate test;
        for(int i=0;i<50000;i++){  // reduce from 100000 since we don't go that high yet on dates
            test = new SQLDate();
            Calendar c = Calendar.getInstance();
                c.add(Calendar.DAY_OF_YEAR,i);
                ts = new Timestamp(c.getTime().getTime());
                des = dvf.getNull(StoredFormatIds.SQL_DATE_ID, StringDataValue.COLLATION_TYPE_UCS_BASIC);
                des.setValue(ts);
                if(des!=null) {
                    test.setValue(des);
                }
            /*
            Assert timestamp and the SQLdate equals, proving that setValue method always return a value
             */
            Assert.assertNotNull(test);
          Assert.assertEquals((new Date(ts.getTime())).toString(),test.toString());
        }

    }


}
