package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.impl.sql.execute.dvd.LazyDouble;
import com.splicemachine.derby.impl.sql.execute.dvd.LazyNumberDataValueDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLDouble;
import org.junit.Assert;
import org.junit.Test;

public class LazyNumberDataValueDescriptorTest {

    @Test
    public void testSetDoubleValue() throws StandardException {

        LazyNumberDataValueDescriptor lndvd = new LazyDouble();
        Assert.assertTrue(lndvd.isNull());

        lndvd.setValue(new Double(3.3));

        Assert.assertEquals(3.3, lndvd.getDouble(), 0.0);

        Assert.assertFalse(lndvd.isNull());

    }
}
