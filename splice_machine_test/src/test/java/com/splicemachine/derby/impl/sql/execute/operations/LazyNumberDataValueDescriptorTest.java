package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.impl.sql.execute.LazyNumberDataValueDescriptor;
import com.splicemachine.derby.impl.sql.execute.serial.DoubleDVDSerializer;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLDouble;
import org.junit.Assert;
import org.junit.Test;

public class LazyNumberDataValueDescriptorTest {

    @Test
    public void testSetDoubleValue() throws StandardException {

        LazyNumberDataValueDescriptor lndvd = new LazyNumberDataValueDescriptor(new SQLDouble());
        Assert.assertTrue(lndvd.isNull());

        lndvd.setValue(new Double(3.3));

        Assert.assertEquals(3.3, lndvd.getDouble(), 0.0);

        Assert.assertFalse(lndvd.isNull());

    }
}
