package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.serial.StringDVDSerializer;
import com.splicemachine.homeless.SerializationUtils;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Ignore;
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

        Assert.assertFalse(ldvdFromBytes.isDeserialized());
        Assert.assertEquals("foo", ldvdFromBytes.getString());
        Assert.assertTrue(ldvdFromBytes.isDeserialized());
        Assert.assertTrue("Foo bytes match after deserialization of whole DVD", Bytes.equals(fooBytes, ldvd.getBytes()));
    }

    @Test
    public void testSerializingWholeDvdNotDeserialized() throws Exception{

        byte[] fooBytes = justBytes("foo");

        LazyDataValueDescriptor ldvd = new LazyDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        ldvd.initForDeserialization(fooBytes);

        Assert.assertFalse(ldvd.isDeserialized());

        LazyDataValueDescriptor newLdvd = SerializationUtils.roundTripObject(ldvd);

        Assert.assertFalse(newLdvd.isDeserialized());
        Assert.assertEquals("foo", newLdvd.getString());
        Assert.assertTrue("Foo bytes match after deserialization of whole DVD", Bytes.equals(fooBytes, newLdvd.getBytes()));
    }

    @Test
    public void testCompareWithoutDeserialization() throws Exception{

        byte[] fooBytes = justBytes("foo");
        byte[] barBytes = justBytes("bar");

        LazyDataValueDescriptor fooDvd = new LazyDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        fooDvd.initForDeserialization(fooBytes);

        LazyDataValueDescriptor barDvd = new LazyDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        barDvd.initForDeserialization(barBytes);

        Assert.assertTrue(fooDvd.getDvd().isNull());
        Assert.assertTrue(barDvd.getDvd().isNull());

        Assert.assertTrue(fooDvd.compare(barDvd) > 0);
        Assert.assertTrue(barDvd.compare(fooDvd) < 0);
        Assert.assertTrue(fooDvd.compare(fooDvd) == 0);


        Assert.assertTrue(fooDvd.getDvd().isNull());
        Assert.assertTrue(barDvd.getDvd().isNull());
    }

    @Test
    public void testCompareWithUnserializedRightSide() throws Exception{

        byte[] fooBytes = justBytes("foo");

        LazyDataValueDescriptor fooDvd = new LazyDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        fooDvd.initForDeserialization(fooBytes);

        LazyDataValueDescriptor barDvd = new LazyDataValueDescriptor(new SQLVarchar("bar"), new StringDVDSerializer());

        Assert.assertTrue(fooDvd.getDvd().isNull());
        Assert.assertFalse(barDvd.isSerialized());

        Assert.assertTrue(fooDvd.compare(barDvd) > 0);
        Assert.assertTrue(barDvd.compare(fooDvd) < 0);
        Assert.assertTrue(fooDvd.compare(fooDvd) == 0);

        Assert.assertTrue(fooDvd.getDvd().isNull());
        Assert.assertTrue(barDvd.isSerialized());
    }

    @Test
    public void testCompareWithNonLazyRightSide() throws Exception{

        byte[] fooBytes = justBytes("foo");

        LazyDataValueDescriptor fooDvd = new LazyDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        fooDvd.initForDeserialization(fooBytes);

        DataValueDescriptor barDvd = new SQLVarchar("bar");

        Assert.assertTrue(fooDvd.getDvd().isNull());

        Assert.assertTrue(fooDvd.compare(barDvd) > 0);
        Assert.assertTrue(fooDvd.compare(fooDvd) == 0);

        Assert.assertFalse(fooDvd.getDvd().isNull());
    }

    @Test
    public void testCompareWithLazyNullsOrderedLow() throws Exception{

        byte[] fooBytes = justBytes("foo");

        LazyDataValueDescriptor fooDvd = new LazyDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        fooDvd.initForDeserialization(fooBytes);

        LazyDataValueDescriptor barDvd = new LazyDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());

        Assert.assertTrue(fooDvd.getDvd().isNull());
        Assert.assertTrue(barDvd.isNull());

        Assert.assertTrue(fooDvd.compare(barDvd, true) > 0);
        Assert.assertTrue(fooDvd.compare(barDvd, false) < 0);
        Assert.assertTrue(barDvd.compare(fooDvd, true) < 0);
        Assert.assertTrue(barDvd.compare(fooDvd, false) > 0);
        Assert.assertTrue(barDvd.compare(barDvd) == 0);

        Assert.assertTrue(fooDvd.getDvd().isNull());
    }

    @Test
    public void testLazyIsNull() throws Exception{

        byte[] fooBytes = justBytes("foo");

        LazyDataValueDescriptor fooDvd = new LazyDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        fooDvd.initForDeserialization(fooBytes);

        Assert.assertTrue(fooDvd.getDvd().isNull());
        Assert.assertFalse(fooDvd.isNull());
        Assert.assertTrue(fooDvd.getDvd().isNull());

    }

    @Test
    public void testLazyGetTypeFormatId() throws Exception{

        byte[] fooBytes = justBytes("foo");

        LazyDataValueDescriptor fooDvd = new LazyDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        fooDvd.initForDeserialization(fooBytes);

        int varcharTypeId = new SQLVarchar().getTypeFormatId();

        Assert.assertTrue(fooDvd.getDvd().isNull());
        Assert.assertEquals(fooDvd.getTypeFormatId(), varcharTypeId);
        Assert.assertTrue(fooDvd.getDvd().isNull());

    }

}
