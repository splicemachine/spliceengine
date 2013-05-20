package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.serial.StringDVDSerializer;
import com.splicemachine.homeless.SerializationUtils;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

public class LazyDataValueDescriptorTest {

    private byte[] justBytes(String value) throws Exception {
        LazyDataValueDescriptor ldvd = new LazyDataValueDescriptor(new SQLVarchar(value), new StringDVDSerializer());
        return ldvd.getBytes();
    }

    @Test
    public void testLazyDeserialization() throws Exception{

        byte[] stringBytes = justBytes("foo");

        LazyDataValueDescriptor ldvd = new LazyDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        ldvd.initForDeserialization(stringBytes);

        Assert.assertTrue(ldvd.getDvd().isNull());

        Assert.assertEquals("foo", ldvd.getString());

        Assert.assertFalse(ldvd.getDvd().isNull());

    }

    @Test
    public void testValuesOnlyDeserializeOnce() throws Exception{

        byte[] stringBytes = justBytes("foo");

        LazyDataValueDescriptor ldvd = new LazyDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        ldvd.initForDeserialization(stringBytes);

        Assert.assertFalse(ldvd.isDeserialized());
        Assert.assertTrue(ldvd.getDvd().isNull());

        Assert.assertEquals("foo", ldvd.getString());

        Assert.assertTrue(ldvd.getString() == ldvd.getString());

        Assert.assertFalse(ldvd.getDvd().isNull());
    }

    @Test
    public void testLazySerialization() throws Exception{

        LazyDataValueDescriptor ldvd = new LazyDataValueDescriptor(new SQLVarchar("foo"), new StringDVDSerializer());

        Assert.assertFalse(ldvd.isSerialized());

        ldvd.getBytes();
        Assert.assertTrue(ldvd.isSerialized());

        LazyDataValueDescriptor ldvd2 = new LazyDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        ldvd2.initForDeserialization(ldvd.getBytes());

        Assert.assertEquals("foo", ldvd2.getString());

    }

    @Test
    public void testReusingDVDForDeserialization() throws Exception{
        byte[] fooBytes = justBytes("foo");
        byte[] barBytes = justBytes("bar");

        LazyDataValueDescriptor ldvd = new LazyDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        ldvd.initForDeserialization(fooBytes);

        Assert.assertTrue(ldvd.getDvd().isNull());
        Assert.assertEquals("foo", ldvd.getString());
        Assert.assertEquals("Bytes still present after forcing deserialization", fooBytes, ldvd.getBytes());

        ldvd.initForDeserialization(barBytes);
        Assert.assertEquals("LDVD should now have bytes representing \"bar\"", barBytes, ldvd.getBytes());
        Assert.assertEquals("bar", ldvd.getString());

    }

    @Test
    public void testSerializingWholeDescriptor() throws Exception{

        byte[] fooBytes = justBytes("foo");

        LazyDataValueDescriptor ldvd = new LazyDataValueDescriptor(new SQLVarchar("foo"), new StringDVDSerializer());

        Assert.assertFalse(ldvd.isSerialized());
        Assert.assertEquals("foo", ldvd.getString());
        Assert.assertTrue("Foo bytes match", Bytes.equals(fooBytes, ldvd.getBytes()));
        Assert.assertTrue(ldvd.isSerialized());

        byte[]  wholeDvdBytes = SerializationUtils.serializeToBytes(ldvd);

        LazyDataValueDescriptor ldvdFromBytes = (LazyDataValueDescriptor) SerializationUtils.deserializeObject(wholeDvdBytes);

        Assert.assertTrue(ldvdFromBytes.isSerialized());
        Assert.assertEquals("foo", ldvdFromBytes.getString());
        Assert.assertTrue("Foo bytes match after deserialization of whole DVD", Bytes.equals(fooBytes, ldvd.getBytes()));
    }

}
