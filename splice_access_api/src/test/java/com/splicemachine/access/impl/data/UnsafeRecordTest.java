package com.splicemachine.access.impl.data;

import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.storage.Record;
import com.splicemachine.utils.IntArrays;
import org.apache.commons.codec.binary.Hex;
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
            0l,true);


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
                0l,true);
        System.out.println(Hex.encodeHexString("rowkey123".getBytes()));
        record.setHasTombstone(true);
        record.setTxnId1(123456l);
        record.setEffectiveTimestamp(123456l);
        record.setNumberOfColumns(123);
        Assert.assertEquals("Display is not accurate",
                "UnsafeRecord {key=726f776b6579313233, version=2, tombstone=true, txnId1=123456, effectiveTimestamp=123456, numberOfColumns=123, columnBitSet=delete}",record.toString());
    }

    @Test
    public void testRecordToValueAndBack() throws Exception {
        UnsafeRecord record = new UnsafeRecord(
                "rowkey123".getBytes(),
                2L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(10)],
                0l,true);
        record.setVersion(123);
        record.setKey("rowkey124".getBytes());
        record.setHasTombstone(true);
        record.setTxnId1(123456l);
        record.setEffectiveTimestamp(123456l);
        record.setNumberOfColumns(2);
        System.out.println("record->" + record);
        record.setData(new DataValueDescriptor[]{new SQLVarchar("123"),new SQLVarchar("123")});
        System.out.println("record2->" + record);
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
                0l,true);
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
        Record rolledBackRecord = records[0].applyRedo(new SingletonIterator(records[1]),rowDefinition);
        ((UnsafeRecord)rolledBackRecord).getData(IntArrays.count(8),rowDefinition);

    }

    @Test
    public void deleteRecordTest() throws Exception {
        UnsafeRecord record = new UnsafeRecord(
                "rowkey123".getBytes(),
                1L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(20)],
                0l,true);
        record.setNumberOfColumns(8);
        record.setTxnId1(100L);
        record.setEffectiveTimestamp(102L);

        UnsafeRecord record2 = new UnsafeRecord(
                "rowkey123".getBytes(),
                2L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(20)],
                0l,false);
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
    @Test
    public void testBlobSerde() throws Exception {
        UnsafeRecord record = new UnsafeRecord(
                "rowkey123".getBytes(),
                1L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(1)],
                0l,true);
        record.setNumberOfColumns(1);
        record.setTxnId1(100L);
        record.setEffectiveTimestamp(0);
        record.setData(new DataValueDescriptor[]{new SQLBlob(Bytes.toBytes("Asdfjkfdsjklfjksdkjfdsljkfljksljkd"))});
        byte[] key = record.getKey();
        byte[] value = record.getValue();

        UnsafeRecord record1 = new UnsafeRecord(key,0,key.length,1l,value,0,true);


        ExecRow newExecRow = new ValueRow(1);
        newExecRow.setRowArray(new DataValueDescriptor[]{new SQLBlob()});
        record1.getData(IntArrays.count(1),newExecRow);
        Assert.assertTrue("blob not serialized correctly",Bytes.equals(newExecRow.getColumn(1).getBytes(),Bytes.toBytes("Asdfjkfdsjklfjksdkjfdsljkfljksljkd")));

    }

    @Test
    public void testAccessedColumnsFromFormatableBitSet() throws Exception {
        UnsafeRecord record = new UnsafeRecord(
                "rowkey123".getBytes(),
                1L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(5)],
                0l,true);
        record.setNumberOfColumns(5);
        record.setTxnId1(100L);
        record.setEffectiveTimestamp(0);
        record.setData(new DataValueDescriptor[]{
                new SQLInteger(1),
                new SQLInteger(2),
                new SQLInteger(3),
                new SQLInteger(4),
                new SQLInteger(5)});
        byte[] key = record.getKey();
        byte[] value = record.getValue();

        UnsafeRecord record1 = new UnsafeRecord(key,0,key.length,1l,value,0,true);
        FormatableBitSet fbs = new FormatableBitSet(5);
        fbs.set(1);
        fbs.set(3);
        ExecRow execRow = new ValueRow(2);
        execRow.setRowArray(new DataValueDescriptor[]{new SQLInteger(),new SQLInteger()});
        record1.getData(fbs,execRow);
        Assert.assertEquals(2,execRow.getColumn(1).getInt());
        Assert.assertEquals(4,execRow.getColumn(2).getInt());
    }

    @Test
    public void userTypeTest() throws Exception {
        UnsafeRecord record = new UnsafeRecord(
                "rowkey123".getBytes(),
                1L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(11)],
                0l,true);
        record.setNumberOfColumns(11);
        record.setTxnId1(100L);
        record.setEffectiveTimestamp(0);
        UnsafeRecord record2 = new UnsafeRecord(
                "rowkey123".getBytes(),
                1L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(11)],
                0l,true);
        record2.setNumberOfColumns(11);
        record2.setTxnId1(100L);
        record2.setEffectiveTimestamp(0);

        ValueRow row = new ValueRow(11);
        row.setColumn(1,new SQLChar("40a68025-015e-ab0e-25c9-00000e263ff0"));
        row.setColumn(2,new SQLVarchar("getSchemas"));
        row.setColumn(3,new SQLChar("40a68025-015e-ab0e-25c9-00000e263ff0"));
        row.setColumn(4,new SQLChar("4"));
        row.setColumn(5,new SQLBoolean(true));
        row.setColumn(6,new SQLLongvarchar("SELECT SCHEMANAME AS TABLE_SCHEM, CAST(NULL AS VARCHAR(128)) AS TABLE_CATALOG FROM SYS.SYSSCHEMAS WHERE ((1=1) OR ? IS NOT NULL) AND SCHEMANAME LIKE ? ORDER BY TABLE_SCHEM"));
        row.setColumn(7,new SQLTimestamp());
        row.setColumn(8,new SQLChar("4"));
        row.setColumn(9,new SQLLongvarchar());
        row.setColumn(10,new UserType(new SQLVarchar("sdfjkdsjkfjkjskdjfkjksdjkfjksjkdjkfkjsdjkkf")));
        row.setColumn(11,new SQLBoolean(false));
        record.setData(row.getRowArray());
        ExecRow row2 = row.getClone();
        record2.setData((FormatableBitSet)null,row2);
        Record[] updates = record.updateRecord(record2,row2);
        ExecRow row_1 = row.getNewNullRow();
        ExecRow row_2 = row.getNewNullRow();
        updates[0].getData((FormatableBitSet)null,row_1);
        System.out.println(row_1);
    }

}