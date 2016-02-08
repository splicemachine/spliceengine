package com.splicemachine.derby.impl.sql.execute.tester;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import com.splicemachine.derby.impl.sql.execute.dvd.LazyDouble;
import com.splicemachine.derby.impl.sql.execute.dvd.LazyTimestamp;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import com.splicemachine.derby.impl.sql.execute.dvd.LazyNumberDataValueDescriptor;
import org.junit.experimental.categories.Category;

import java.sql.Timestamp;

/**
 * TODO: Should be more tests here for LazyTimestampDataValueDescriptor. Adding this test for verification of fix for DB-2863.
 *
 * @author Jeff Cunningham
 *         Date: 2/13/15
 */
@Category(ArchitectureIndependent.class)
public class LazyTimestampTest{

    @Test
    public void testGetSeconds() throws Exception {
        int expected = 32;
        LazyTimestamp timestamp = new LazyTimestamp(new SQLTimestamp(new DateTime(2012, 2, 14, 12, 12, expected, 0)));
        double actual = timestamp.getSeconds(new LazyDouble(new SQLDouble(0.0))).getDouble();
        Assert.assertEquals(expected, actual, 0.0);
    }


    @Test
    public void testBounds() throws StandardException {
        Timestamp tsMin = new Timestamp(SQLTimestamp.MIN_TIMESTAMP - 1);
        Timestamp tsMax = new Timestamp(SQLTimestamp.MAX_TIMESTAMP + 1);
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
