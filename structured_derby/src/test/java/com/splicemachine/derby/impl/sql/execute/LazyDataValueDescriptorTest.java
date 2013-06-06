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

        Assert.assertTrue(fooDvd.getDvd().isNull());
        Assert.assertTrue(barDvd.getDvd().isNull());
    }

    @Ignore
    @Test
    public void testPerfCompare() throws Exception {
        String string1 = new String("abcdefghijklmnopqrstuvwxyz");
        String string2 = new String("abcdefghijklmnopqrstuvwxyz1");

        byte[] bytes1 = justBytes(string1);
        byte[] bytes2 = justBytes(string2);

        SQLVarchar sv1 = new SQLVarchar(string1);
        SQLVarchar sv2 = new SQLVarchar(string2);


        int x = 0;

        long b1 = System.currentTimeMillis();

        for(int i=0; i < 1000000000; i++){

            if(Bytes.compareTo(bytes1, bytes2) < 0){
                x++;
            }

        }
        long a1 = System.currentTimeMillis();

        int y = 0;

        long b2 = System.currentTimeMillis();

        for(int i=0; i < 1000000000; i++){

            if(string1.compareTo(string2) < 0){
                y++;
            }

        }
        long a2 = System.currentTimeMillis();

        int z = 0;

        long b3 = System.currentTimeMillis();

        for(int i=0; i < 1000000000; i++){

            if(sv1.compareTo(sv2) < 0){
                z++;
            }

        }
        long a3 = System.currentTimeMillis();

        System.out.println("Same results? " +  (x == y  && x == z) + " x: " + x + " y: " + y + " z: " + z);
        System.out.println((a1 - b1) + " vs " + (a2 - b2) + " vs " + (a3-b3));
    }

}
