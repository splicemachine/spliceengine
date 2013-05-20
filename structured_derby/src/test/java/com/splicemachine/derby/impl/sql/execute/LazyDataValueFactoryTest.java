package com.splicemachine.derby.impl.sql.execute;

import org.apache.derby.iapi.types.DataValueFactory;
import org.junit.Assert;
import org.junit.Test;

public class LazyDataValueFactoryTest {

    private DataValueFactory lfac = new LazyDataValueFactory();

    @Test
    public void testGetVarcharDataValue() throws Exception {
        String value = "foo";

        LazyDataValueDescriptor dvd = (LazyDataValueDescriptor) lfac.getVarcharDataValue(value);
        LazyDataValueDescriptor dvd2 = (LazyDataValueDescriptor) lfac.getVarcharDataValue(value);


        Assert.assertEquals(value, dvd.getString());
        Assert.assertEquals(value, dvd2.getString());

        Assert.assertFalse(dvd.getDVDSerializer() == dvd2.getDVDSerializer());
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

    @Test
    public void testGetVarcharDataValueNewSerializerInstance() throws Exception {
        LazyStringDataValueDescriptor prev = (LazyStringDataValueDescriptor) lfac.getVarcharDataValue("foo");
        LazyStringDataValueDescriptor prev2 = (LazyStringDataValueDescriptor) lfac.getVarcharDataValue("foo");

        Assert.assertFalse(prev.getDVDSerializer() == prev2.getDVDSerializer());

        LazyStringDataValueDescriptor dvd = (LazyStringDataValueDescriptor) lfac.getVarcharDataValue("bar", prev);
        LazyStringDataValueDescriptor dvd2 = (LazyStringDataValueDescriptor) lfac.getVarcharDataValue("bar", prev2);


        Assert.assertFalse(dvd.getDVDSerializer() == dvd2.getDVDSerializer());

    }

}
