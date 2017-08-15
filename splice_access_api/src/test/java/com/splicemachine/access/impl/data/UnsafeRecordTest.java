package com.splicemachine.access.impl.data;

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.storage.Record;
import com.splicemachine.utils.IntArrays;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 *
 */
public class UnsafeRecordTest {
    public static UnsafeRecord TEST_RECORD = new UnsafeRecord(
            "rowkey123".getBytes(),
    2L,
            new byte[UnsafeRecordUtils.calculateFixedRecordSize(10)],
            16l,true);


    @Test
    public void testRowKeyMutability() throws Exception {
        Assert.assertEquals("RowKey Not Set", Bytes.toString(TEST_RECORD.getKey()),"rowkey123");
        TEST_RECORD.setKey("rowkey124".getBytes());
        Assert.assertEquals("RowKey Not Set", Bytes.toString(TEST_RECORD.getKey()),"rowkey124");
    }

    @Test
    public void testTombstoneMutability() throws Exception {
        TEST_RECORD.setHasTombstone(true);
        Assert.assertTrue("Tombstone Not Set",TEST_RECORD.hasTombstone());
        TEST_RECORD.setHasTombstone(false);
        Assert.assertFalse("Tombstone Set",TEST_RECORD.hasTombstone());
    }

    @Test
    public void testTxnID1Mutability() throws Exception {
        TEST_RECORD.setTxnId1(123456l);
        Assert.assertEquals("Transaction Id1 Not Set",TEST_RECORD.getTxnId1(),123456l);
        TEST_RECORD.setTxnId1(123l);
        Assert.assertEquals("Transaction Id1 Not Set",TEST_RECORD.getTxnId1(),123l);
    }

    @Test
    public void testEffectiveTsMutability() throws Exception {
        TEST_RECORD.setEffectiveTimestamp(123456l);
        Assert.assertEquals("Effective Timestamp Not Set",TEST_RECORD.getEffectiveTimestamp(),123456l);
        TEST_RECORD.setEffectiveTimestamp(123l);
        Assert.assertEquals("Effective Timestamp Not Set",TEST_RECORD.getEffectiveTimestamp(),123l);
    }

    @Test
    public void testNumberOfColumnsMutability() throws Exception {
        TEST_RECORD.setNumberOfColumns(123);
        Assert.assertEquals("Number of Columns Not Set",TEST_RECORD.numberOfColumns(),123);
        TEST_RECORD.setNumberOfColumns(12);
        Assert.assertEquals("Number of Columns Not Set",TEST_RECORD.numberOfColumns(),12);

    }

    @Test
    public void testRecordToString() {
        UnsafeRecord record = new UnsafeRecord(
                "rowkey123".getBytes(),
                2L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(10)],
                16l,true);
        record.setKey("rowkey124".getBytes());
        record.setHasTombstone(true);
        record.setTxnId1(123456l);
        record.setEffectiveTimestamp(123456l);
        record.setNumberOfColumns(123);
        Assert.assertEquals("Display is not accurate","UnsafeRecord {key=726f776b6579313234, version=2, " +
                "tombstone=true, txnId1=123456, effectiveTimestamp=123456, numberOfColumns=123}",record.toString());
    }

    @Test
    public void testRecordToValueAndBack() throws Exception {
        UnsafeRecord record = new UnsafeRecord(
                "rowkey123".getBytes(),
                2L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(10)],
                16l,true);
        record.setVersion(123);
        record.setKey("rowkey124".getBytes());
        record.setHasTombstone(true);
        record.setTxnId1(123456l);
        record.setEffectiveTimestamp(123456l);
        record.setNumberOfColumns(2);
        record.setData(new DataValueDescriptor[]{new SQLVarchar("123"),new SQLVarchar("123")});
        ValueRow foo = new ValueRow(2);
        foo.setRowArray(new DataValueDescriptor[]{new SQLVarchar(""),new SQLVarchar("")});
        record.getData(new int[]{0,1},foo);

        byte[] value = record.getValue();
        byte[] key = record.getKey();
        UnsafeRecord hmm = new UnsafeRecord(key,0,key.length,123,value,0,true);
        foo.setRowArray(new DataValueDescriptor[]{new SQLVarchar(""),new SQLVarchar("")});
        hmm.getData(new int[]{0,1},foo);
    }


    @Test
    public void testDataSerDe() throws Exception{
        UnsafeRecord record = new UnsafeRecord(
                "rowkey123".getBytes(),
                2L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(10)],
                16l,true);
        record.setNumberOfColumns(12);
        ValueRow valueRow = new ValueRow(2);
        valueRow.setRowArray(new DataValueDescriptor[]{
                new SQLVarchar("1234"),new SQLInteger(12)});
        record.setData(new int[]{2,4},valueRow);
        ValueRow valueRow2 = new ValueRow(2);
        valueRow2.setRowArray(new DataValueDescriptor[]{
                new SQLVarchar(),new SQLInteger()});
        record.getData(new int[]{2,4},valueRow2);
        Assert.assertEquals("column 1 comparison",valueRow.getColumn(1).getString(),valueRow2.getColumn(1).getString());
        Assert.assertEquals("column 2 comparison",valueRow.getColumn(2).getString(),valueRow2.getColumn(2).getString());
    }

    @Test
    public void updateRecordTest() throws Exception {
        UnsafeRecord record = new UnsafeRecord(
                "rowkey123".getBytes(),
                1L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(20)],
                16l,true);
        record.setNumberOfColumns(8);
        record.setTxnId1(100L);
        record.setEffectiveTimestamp(102L);

        UnsafeRecord record2 = new UnsafeRecord(
                "rowkey123".getBytes(),
                2L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(20)],
                16l,false);
        record2.setNumberOfColumns(8);
        record2.setTxnId1(200L);
        record2.setEffectiveTimestamp(0l);

        ValueRow valueRow = new ValueRow(4);
        valueRow.setRowArray(new DataValueDescriptor[]{new SQLVarchar("1234"),new SQLInteger(12),new SQLInteger(13),new SQLVarchar()});
        record.setData(new int[]{2,4,5,6},valueRow);
        ValueRow valueRow2 = new ValueRow(4);
        valueRow2.setRowArray(new DataValueDescriptor[]{new SQLVarchar("FOOEY"),new SQLInteger(123),new SQLInteger(),new SQLVarchar()});
        record2.setData(new int[]{2,3,5,6},valueRow2);
        ValueRow rowDefinition = new ValueRow(8);
        rowDefinition.setRowArray(new DataValueDescriptor[]{new SQLInteger(), new SQLInteger(),
                new SQLVarchar(), new SQLInteger(), new SQLInteger(), new SQLInteger(),
                new SQLVarchar(), new SQLInteger()});
        Record[] records = record.updateRecord(record2,rowDefinition);
        ((UnsafeRecord)records[0]).getData(IntArrays.count(8),rowDefinition);
        Assert.assertEquals("Column 2 Set Incorrectly","FOOEY",rowDefinition.getColumn(3).getString());
        Assert.assertEquals("Column 3 Set Incorrectly",123,rowDefinition.getColumn(4).getInt());
        Assert.assertEquals("Column 4 Set Incorrectly",12,rowDefinition.getColumn(5).getInt());
        ((UnsafeRecord)records[1]).getData(IntArrays.count(8),rowDefinition);
        Assert.assertEquals("Column 2 Set Incorrectly","1234",rowDefinition.getColumn(3).getString());
        Assert.assertEquals("Column 3 Set Incorrectly",true,rowDefinition.getColumn(4).isNull());
        Assert.assertEquals("Column 5 Set Incorrectly",13,rowDefinition.getColumn(6).getInt());
        Record rolledBackRecord = records[0].applyRollback(new SingletonIterator(records[1]),rowDefinition);
        ((UnsafeRecord)rolledBackRecord).getData(IntArrays.count(8),rowDefinition);

    }

    @Test
    public void deleteRecordTest() throws Exception {
        UnsafeRecord record = new UnsafeRecord(
                "rowkey123".getBytes(),
                1L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(20)],
                16l,true);
        record.setNumberOfColumns(8);
        record.setTxnId1(100L);
        record.setEffectiveTimestamp(102L);

        UnsafeRecord record2 = new UnsafeRecord(
                "rowkey123".getBytes(),
                2L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(20)],
                16l,false);
        record2.setNumberOfColumns(8);
        record2.setTxnId1(200L);
        record2.setEffectiveTimestamp(0l);
        record2.setHasTombstone(true);

        ValueRow valueRow = new ValueRow(4);
        valueRow.setRowArray(new DataValueDescriptor[]{new SQLVarchar("1234"),new SQLInteger(12),new SQLInteger(13),new SQLVarchar()});
        record.setData(new int[]{2,4,5,6},valueRow);
        ValueRow rowDefinition = new ValueRow(8);
        rowDefinition.setRowArray(new DataValueDescriptor[]{new SQLInteger(), new SQLInteger(),
                new SQLVarchar(), new SQLInteger(), new SQLInteger(), new SQLInteger(),
                new SQLVarchar(), new SQLInteger()});
        Record[] records = record.updateRecord(record2,rowDefinition);
        Assert.assertTrue("First Record Not Deleted",records[0].hasTombstone());
    }

}