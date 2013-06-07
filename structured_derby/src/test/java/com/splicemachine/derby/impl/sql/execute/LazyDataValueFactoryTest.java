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

        Assert.assertTrue(dvd.getDVDSerializer() == dvd2.getDVDSerializer());
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

        LazyStringDataValueDescriptor dvd1 = (LazyStringDataValueDescriptor) lfac.getVarcharDataValue("foo");
        final DVDSerializer serializer1 = dvd1.getDVDSerializer();

        LazyStringDataValueDescriptor dvd2 = (LazyStringDataValueDescriptor) lfac.getVarcharDataValue("foo");
        final DVDSerializer serializer2 = dvd1.getDVDSerializer();

        //Getting a DVDSerializer from the same thread should get the same instance
        Assert.assertTrue(serializer1 == serializer2);

        final AtomicBoolean didTestRun = new AtomicBoolean(false);

        Thread thread = new Thread(){
            @Override
            public void run() {

                LazyStringDataValueDescriptor dvd3 = (LazyStringDataValueDescriptor) lfac.getVarcharDataValue("foo");
                DVDSerializer serializer3 = dvd3.getDVDSerializer();

                //On a different thread, it should create a new serializer and use that one
                Assert.assertFalse(serializer1 == serializer3);

                didTestRun.compareAndSet(false, true);

            }
        };

        thread.start();
        thread.join(10000);

        Assert.assertTrue("Thread to get a new serializer didn't run", didTestRun.get());
    }

}
