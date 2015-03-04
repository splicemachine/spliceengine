package com.splicemachine.derby.impl.sql.execute.tester;

import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import com.splicemachine.derby.impl.sql.execute.LazyNumberDataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.LazyTimestampDataValueDescriptor;

/**
 * TODO: Should be more tests here for LazyTimestampDataValueDescriptor. Adding this test for verification of fix for DB-2863.
 *
 * @author Jeff Cunningham
 *         Date: 2/13/15
 */
public class LazyTimestampDataValueDescriptorTest {

    @Test
    public void testGetSeconds() throws Exception {
        int expected = 32;
        LazyTimestampDataValueDescriptor timestamp =
            new LazyTimestampDataValueDescriptor(new SQLTimestamp(new DateTime(2012, 2, 14, 12, 12, expected, 0)));
        double actual = timestamp.getSeconds(new LazyNumberDataValueDescriptor(new SQLDouble(0.0))).getDouble();
        Assert.assertEquals(expected, actual, 0.0);
    }
}
