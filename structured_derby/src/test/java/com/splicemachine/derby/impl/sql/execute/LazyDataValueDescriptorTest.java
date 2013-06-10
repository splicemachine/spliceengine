package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.serial.StringDVDSerializer;
import com.splicemachine.homeless.SerializationUtils;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

public class LazyDataValueDescriptorTest {

    private byte[] justBytes(String value) throws Exception {
        LazyStringDataValueDescriptor ldvd = new LazyStringDataValueDescriptor(new SQLVarchar(value), new StringDVDSerializer());
        return ldvd.getBytes();
    }

    @Test
    public void testLazyDeserialization() throws Exception{

        byte[] stringBytes = justBytes("foo");

        LazyStringDataValueDescriptor ldvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        ldvd.initForDeserialization(stringBytes);

        Assert.assertTrue(ldvd.sdv.isNull());

        Assert.assertEquals("foo", ldvd.getString());

        Assert.assertFalse(ldvd.sdv.isNull());

    }

    @Test
    public void testValuesOnlyDeserializeOnce() throws Exception{

        byte[] stringBytes = justBytes("foo");

        LazyStringDataValueDescriptor ldvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        ldvd.initForDeserialization(stringBytes);

        Assert.assertFalse(ldvd.isDeserialized());
        Assert.assertTrue(ldvd.sdv.isNull());

        Assert.assertEquals("foo", ldvd.getString());

        Assert.assertTrue(ldvd.getString() == ldvd.getString());

        Assert.assertFalse(ldvd.sdv.isNull());
    }

    @Test
    public void testLazySerialization() throws Exception{

        LazyStringDataValueDescriptor ldvd = new LazyStringDataValueDescriptor(new SQLVarchar("foo"), new StringDVDSerializer());

        Assert.assertFalse(ldvd.isSerialized());

        ldvd.getBytes();
        Assert.assertTrue(ldvd.isSerialized());

        LazyStringDataValueDescriptor ldvd2 = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        ldvd2.initForDeserialization(ldvd.getBytes());

        Assert.assertEquals("foo", ldvd2.getString());

    }

    @Test
    public void testReusingDVDForDeserialization() throws Exception{
        byte[] fooBytes = justBytes("foo");
        byte[] barBytes = justBytes("bar");

        LazyStringDataValueDescriptor ldvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        ldvd.initForDeserialization(fooBytes);

        Assert.assertTrue(ldvd.sdv.isNull());
        Assert.assertEquals("foo", ldvd.getString());
        Assert.assertEquals("Bytes still present after forcing deserialization", fooBytes, ldvd.getBytes());

        ldvd.initForDeserialization(barBytes);
        Assert.assertEquals("LDVD should now have bytes representing \"bar\"", barBytes, ldvd.getBytes());
        Assert.assertEquals("bar", ldvd.getString());

    }

    @Test
     public void testSerializingWholeDescriptor() throws Exception{

        byte[] fooBytes = justBytes("foo");

        LazyStringDataValueDescriptor ldvd = new LazyStringDataValueDescriptor(new SQLVarchar("foo"), new StringDVDSerializer());

        Assert.assertFalse(ldvd.isSerialized());
        Assert.assertEquals("foo", ldvd.getString());
        Assert.assertTrue("Foo bytes match", Bytes.equals(fooBytes, ldvd.getBytes()));
        Assert.assertTrue(ldvd.isSerialized());

        byte[]  wholeDvdBytes = SerializationUtils.serializeToBytes(ldvd);

        LazyStringDataValueDescriptor ldvdFromBytes = (LazyStringDataValueDescriptor) SerializationUtils.deserializeObject(wholeDvdBytes);

        Assert.assertFalse(ldvdFromBytes.isDeserialized());
        Assert.assertEquals("foo", ldvdFromBytes.getString());
        Assert.assertTrue(ldvdFromBytes.isDeserialized());
        Assert.assertTrue("Foo bytes match after deserialization of whole DVD", Bytes.equals(fooBytes, ldvd.getBytes()));
    }

    @Test
    public void testSerializingWholeDvdNotDeserialized() throws Exception{

        byte[] fooBytes = justBytes("foo");

        LazyStringDataValueDescriptor ldvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        ldvd.initForDeserialization(fooBytes);

        Assert.assertFalse(ldvd.isDeserialized());

        LazyStringDataValueDescriptor newLdvd = SerializationUtils.roundTripObject(ldvd);

        Assert.assertFalse(newLdvd.isDeserialized());
        Assert.assertEquals("foo", newLdvd.getString());
        Assert.assertTrue("Foo bytes match after deserialization of whole DVD", Bytes.equals(fooBytes, newLdvd.getBytes()));
    }

    @Test
    public void testCompareWithoutDeserialization() throws Exception{

        byte[] fooBytes = justBytes("foo");
        byte[] barBytes = justBytes("bar");

        LazyStringDataValueDescriptor fooDvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        fooDvd.initForDeserialization(fooBytes);

        LazyStringDataValueDescriptor barDvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        barDvd.initForDeserialization(barBytes);

        Assert.assertTrue(fooDvd.sdv.isNull());
        Assert.assertTrue(barDvd.sdv.isNull());

        Assert.assertTrue(fooDvd.compare(barDvd) > 0);
        Assert.assertTrue(barDvd.compare(fooDvd) < 0);
        Assert.assertTrue(fooDvd.compare(fooDvd) == 0);


        Assert.assertTrue(fooDvd.sdv.isNull());
        Assert.assertTrue(barDvd.sdv.isNull());
    }

    @Test
    public void testCompareWithUnserializedRightSide() throws Exception{

        byte[] fooBytes = justBytes("foo");

        LazyStringDataValueDescriptor fooDvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        fooDvd.initForDeserialization(fooBytes);

        LazyStringDataValueDescriptor barDvd = new LazyStringDataValueDescriptor(new SQLVarchar("bar"), new StringDVDSerializer());

        Assert.assertTrue(fooDvd.sdv.isNull());
        Assert.assertFalse(barDvd.isSerialized());

        Assert.assertTrue(fooDvd.compare(barDvd) > 0);
        Assert.assertTrue(barDvd.compare(fooDvd) < 0);
        Assert.assertTrue(fooDvd.compare(fooDvd) == 0);

        Assert.assertTrue(fooDvd.sdv.isNull());
        Assert.assertTrue(barDvd.isSerialized());
    }

    @Test
    public void testCompareWithNonLazyRightSide() throws Exception{

        byte[] fooBytes = justBytes("foo");

        LazyStringDataValueDescriptor fooDvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        fooDvd.initForDeserialization(fooBytes);

        DataValueDescriptor barDvd = new SQLVarchar("bar");

        Assert.assertTrue(fooDvd.sdv.isNull());

        Assert.assertTrue(fooDvd.compare(barDvd) > 0);
        Assert.assertTrue(fooDvd.compare(fooDvd) == 0);

        Assert.assertFalse(fooDvd.sdv.isNull());
    }

    @Test
     public void testCompareWithLazyNullsOrderedLow() throws Exception{

        byte[] fooBytes = justBytes("foo");

        LazyStringDataValueDescriptor fooDvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        fooDvd.initForDeserialization(fooBytes);

        LazyStringDataValueDescriptor barDvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());

        Assert.assertTrue(fooDvd.sdv.isNull());
        Assert.assertTrue(barDvd.isNull());

        Assert.assertTrue(fooDvd.compare(barDvd, true) > 0);
        Assert.assertTrue(fooDvd.compare(barDvd, false) < 0);
        Assert.assertTrue(barDvd.compare(fooDvd, true) < 0);
        Assert.assertTrue(barDvd.compare(fooDvd, false) > 0);
        Assert.assertTrue(barDvd.compare(barDvd) == 0);

        Assert.assertTrue(fooDvd.sdv.isNull());
    }

    @Test
    public void testCompareWithOperator() throws Exception{

        byte[] fooBytes = justBytes("foo");
        byte[] barBytes = justBytes("bar");

        LazyStringDataValueDescriptor fooDvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        fooDvd.initForDeserialization(fooBytes);

        LazyStringDataValueDescriptor barDvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        barDvd.initForDeserialization(barBytes);

        Assert.assertTrue(fooDvd.sdv.isNull());
        Assert.assertTrue(barDvd.sdv.isNull());

        Assert.assertTrue(fooDvd.compare(Orderable.ORDER_OP_GREATEROREQUALS, barDvd, false, false));
        Assert.assertTrue(fooDvd.compare(Orderable.ORDER_OP_GREATERTHAN, barDvd, false, false));
        Assert.assertFalse(fooDvd.compare(Orderable.ORDER_OP_EQUALS, barDvd, false, false));
        Assert.assertFalse(fooDvd.compare(Orderable.ORDER_OP_LESSTHAN, barDvd, false, false));
        Assert.assertFalse(fooDvd.compare(Orderable.ORDER_OP_LESSOREQUALS, barDvd, false, false));

        Assert.assertTrue(fooDvd.compare(Orderable.ORDER_OP_EQUALS, fooDvd, false, false));
        Assert.assertTrue(fooDvd.compare(Orderable.ORDER_OP_LESSOREQUALS, fooDvd, false, false));
        Assert.assertTrue(fooDvd.compare(Orderable.ORDER_OP_GREATEROREQUALS, fooDvd, false, false));

        Assert.assertFalse(barDvd.compare(Orderable.ORDER_OP_GREATEROREQUALS, fooDvd, false, false));
        Assert.assertFalse(barDvd.compare(Orderable.ORDER_OP_GREATERTHAN, fooDvd, false, false));
        Assert.assertFalse(barDvd.compare(Orderable.ORDER_OP_EQUALS, fooDvd, false, false));
        Assert.assertTrue(barDvd.compare(Orderable.ORDER_OP_LESSTHAN, fooDvd, false, false));
        Assert.assertTrue(barDvd.compare(Orderable.ORDER_OP_LESSOREQUALS, fooDvd, false, false));

        Assert.assertTrue(fooDvd.sdv.isNull());
        Assert.assertTrue(barDvd.sdv.isNull());
    }

    @Test
    public void testCompareWithOperatorAndOrderedNulls() throws Exception{

        byte[] fooBytes = justBytes("foo");

        LazyStringDataValueDescriptor fooDvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        fooDvd.initForDeserialization(fooBytes);

        LazyStringDataValueDescriptor barDvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());

        Assert.assertTrue(fooDvd.sdv.isNull());
        Assert.assertTrue(barDvd.isNull());

        Assert.assertTrue(fooDvd.compare(Orderable.ORDER_OP_GREATEROREQUALS, barDvd, false, true, false));
        Assert.assertTrue(fooDvd.compare(Orderable.ORDER_OP_GREATERTHAN, barDvd, false, true, false));
        Assert.assertFalse(fooDvd.compare(Orderable.ORDER_OP_EQUALS, barDvd, false, true, false));
        Assert.assertFalse(fooDvd.compare(Orderable.ORDER_OP_LESSTHAN, barDvd, false, true, false));
        Assert.assertFalse(fooDvd.compare(Orderable.ORDER_OP_LESSOREQUALS, barDvd, false, true, false));

        Assert.assertFalse(fooDvd.compare(Orderable.ORDER_OP_GREATEROREQUALS, barDvd, false, false, false));
        Assert.assertFalse(fooDvd.compare(Orderable.ORDER_OP_GREATERTHAN, barDvd, false, false, false));
        Assert.assertFalse(fooDvd.compare(Orderable.ORDER_OP_EQUALS, barDvd, false, false, false));
        Assert.assertTrue(fooDvd.compare(Orderable.ORDER_OP_LESSTHAN, barDvd, false, false, false));
        Assert.assertTrue(fooDvd.compare(Orderable.ORDER_OP_LESSOREQUALS, barDvd, false, false, false));

        Assert.assertTrue(fooDvd.sdv.isNull());
    }

    @Test
    public void testTopLevelComparisonMethods() throws Exception{

        byte[] fooBytes = justBytes("foo");
        byte[] barBytes = justBytes("bar");

        LazyStringDataValueDescriptor fooDvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        fooDvd.initForDeserialization(fooBytes);

        LazyStringDataValueDescriptor barDvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        barDvd.initForDeserialization(barBytes);

        Assert.assertTrue(fooDvd.sdv.isNull());
        Assert.assertTrue(barDvd.sdv.isNull());

        assertSqlTrue(fooDvd.greaterThan(fooDvd, barDvd));
        assertSqlTrue(fooDvd.greaterOrEquals(fooDvd, barDvd));
        assertSqlFalse(fooDvd.lessThan(fooDvd, barDvd));
        assertSqlFalse(fooDvd.lessOrEquals(fooDvd, barDvd));
        assertSqlFalse(fooDvd.equals(fooDvd, barDvd));
        assertSqlTrue(fooDvd.notEquals(fooDvd, barDvd));

        assertSqlTrue(fooDvd.equals(fooDvd, fooDvd));
        assertSqlFalse(fooDvd.notEquals(fooDvd, fooDvd));

        Assert.assertTrue(fooDvd.sdv.isNull());
        Assert.assertTrue(barDvd.sdv.isNull());

    }

    private void assertSqlTrue(BooleanDataValue sqlBool){
        Assert.assertTrue(sqlBool.getBoolean());
    }

    private void assertSqlFalse(BooleanDataValue sqlBool){
        Assert.assertFalse(sqlBool.getBoolean());
    }


    @Test
    public void testLazyIsNull() throws Exception{

        byte[] fooBytes = justBytes("foo");

        LazyStringDataValueDescriptor fooDvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        fooDvd.initForDeserialization(fooBytes);

        Assert.assertTrue(fooDvd.sdv.isNull());
        Assert.assertFalse(fooDvd.isNull());
        Assert.assertTrue(fooDvd.sdv.isNull());

    }

    @Test
    public void testLazyGetTypeFormatId() throws Exception{

        byte[] fooBytes = justBytes("foo");

        LazyStringDataValueDescriptor fooDvd = new LazyStringDataValueDescriptor(new SQLVarchar(), new StringDVDSerializer());
        fooDvd.initForDeserialization(fooBytes);

        int varcharTypeId = new SQLVarchar().getTypeFormatId();

        Assert.assertTrue(fooDvd.sdv.isNull());
        Assert.assertEquals(fooDvd.getTypeFormatId(), varcharTypeId);
        Assert.assertTrue(fooDvd.sdv.isNull());

    }

}
