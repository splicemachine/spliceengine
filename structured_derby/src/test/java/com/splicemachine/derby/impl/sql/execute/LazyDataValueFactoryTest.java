package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.serial.DVDSerializer;
import com.splicemachine.derby.impl.sql.execute.serial.StringDVDSerializer;
import org.apache.derby.iapi.types.DataValueFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LazyDataValueFactoryTest {

    private final DataValueFactory lfac = new LazyDataValueFactory();

    @Test
    public void testGetVarcharDataValue() throws Exception {
        String value = "foo";

        LazyDataValueDescriptor dvd = (LazyDataValueDescriptor) lfac.getVarcharDataValue(value);
        LazyDataValueDescriptor dvd2 = (LazyDataValueDescriptor) lfac.getVarcharDataValue(value);


        Assert.assertEquals(value, dvd.getString());
        Assert.assertEquals(value, dvd2.getString());

    }

    @Test
    public void testGetVarcharDataValueWithPrevious() throws Exception {

        LazyStringDataValueDescriptor dvd = (LazyStringDataValueDescriptor) lfac.getVarcharDataValue("foo");

        Assert.assertEquals("foo", dvd.getString());

        LazyStringDataValueDescriptor dvd2 = (LazyStringDataValueDescriptor) lfac.getVarcharDataValue("bar", dvd);

        Assert.assertEquals(dvd.getString(), "bar");
        Assert.assertEquals(dvd2.getString(), "bar");

        Assert.assertTrue(dvd == dvd2);
    }

}
