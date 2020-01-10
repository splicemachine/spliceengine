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

package com.splicemachine.derby.impl.sql.execute.tester;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Timestamp;

/**
 * TODO: Should be more tests here for LazyTimestampDataValueDescriptor. Adding this test for verification of fix for DB-2863.
 *
 * @author Jeff Cunningham
 *         Date: 2/13/15
 */
@Category(ArchitectureIndependent.class)
public class TimestampTest {

    @Ignore("Version 2.0 Timestamp bounds were removed by SPLICE-1212.")
    @Test
    public void testBounds() throws StandardException {
        Timestamp tsMin = new Timestamp(SQLTimestamp.MIN_V2_TIMESTAMP - 1);
        Timestamp tsMax = new Timestamp(SQLTimestamp.MAX_V2_TIMESTAMP + 1);
        Timestamp tsOk = new Timestamp(0);

        SQLTimestamp sts = new SQLTimestamp();

        try {
            sts.setValue(tsMin);
            Assert.fail("No exception about bounds");
        } catch (StandardException e) {
            Assert.assertEquals(SQLState.LANG_DATE_TIME_ARITHMETIC_OVERFLOW, e.getSqlState());
        }

        try {
            sts.setValue(tsMax);
            Assert.fail("No exception about bounds");
        } catch (StandardException e) {
            Assert.assertEquals(SQLState.LANG_DATE_TIME_ARITHMETIC_OVERFLOW, e.getSqlState());
        }

        sts.setValue(tsOk);
    }
}
