package com.splicemachine.access.impl.data;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.storage.Record;
import com.splicemachine.utils.IntArrays;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 *
 */
public class UnsafeRecordTest {

    @Test
    public void testRecordCreationAndMutability() throws Exception {
        UnsafeRecord record = new UnsafeRecord(
                "rowkey123".getBytes(),
                2L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(10)],
                16l,true);
        Assert.assertEquals("RowKey Not Set", Bytes.toString(record.getKey()),"rowkey123");
        record.setKey("rowkey124".getBytes());
        Assert.assertEquals("RowKey Not Set", Bytes.toString(record.getKey()),"rowkey124");
        record.setHasTombstone(true);
        Assert.assertTrue("Tombstone Not Set",record.hasTombstone());
        record.setHasTombstone(false);
        Assert.assertFalse("Tombstone Set",record.hasTombstone());
        record.setTxnId1(123456l);
        Assert.assertEquals("Transaction Id1 Not Set",record.getTxnId1(),123456l);
        record.setTxnId1(123l);
        Assert.assertEquals("Transaction Id1 Not Set",record.getTxnId1(),123l);
        record.setTxnId2(123456l);
        Assert.assertEquals("Transaction Id2 Not Set",record.getTxnId2(),123456l);
        record.setTxnId2(123l);
        Assert.assertEquals("Transaction Id2 Not Set",record.getTxnId2(),123l);
        record.setEffectiveTimestamp(123456l);
        Assert.assertEquals("Effective Timestamp Not Set",record.getEffectiveTimestamp(),123456l);
        record.setEffectiveTimestamp(123l);
        Assert.assertEquals("Effective Timestamp Not Set",record.getEffectiveTimestamp(),123l);
        record.setNumberOfColumns(123);
        Assert.assertEquals("Number of Columns Not Set",record.numberOfColumns(),123);
        record.setNumberOfColumns(12);
        Assert.assertEquals("Number of Columns Not Set",record.numberOfColumns(),12);
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
        record.setTxnId2(123456l);
        record.setEffectiveTimestamp(123456l);
        record.setNumberOfColumns(123);
        Assert.assertEquals("Display is not accurate","UnsafeRecord {key=726f776b6579313234, version=2, tombstone=true, txnId1=123456, txnId2=123456, effectiveTimestamp=123456, numberOfColumns=123}",record.toString());
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
        valueRow.setColumn(1,new SQLVarchar("1234"));
        valueRow.setColumn(2,new SQLInteger(12));
        record.setData(new int[]{2,4},valueRow);
        ValueRow valueRow2 = new ValueRow(2);
        valueRow2.setColumn(1,new SQLVarchar());
        valueRow2.setColumn(2,new SQLInteger());

        ExecRow foo = record.getData(new int[]{2,4},valueRow2);
        Assert.assertEquals("column 1 comparison",valueRow.getColumn(1).getString(),foo.getColumn(1).getString());
        Assert.assertEquals("column 2 comparison",valueRow.getColumn(2).getString(),foo.getColumn(2).getString());
    }

    @Test
    public void updateRecordTest() throws Exception {
        UnsafeRecord record = new UnsafeRecord(
                "rowkey123".getBytes(),
                2L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(20)],
                16l,true);
        record.setNumberOfColumns(6);
        record.setTxnId1(1234L);
        record.setTxnId1(1235L);
        record.setEffectiveTimestamp(0l);


        UnsafeRecord record2 = new UnsafeRecord(
                "rowkey123".getBytes(),
                2L,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(20)],
                16l,false);
        record2.setNumberOfColumns(6);
        record2.setTxnId1(1234L);
        record2.setTxnId1(1235L);
        record2.setEffectiveTimestamp(0l);
        ValueRow valueRow = new ValueRow(4);
        valueRow.setColumn(1,new SQLVarchar("1234"));
        valueRow.setColumn(2,new SQLInteger(12));
        valueRow.setColumn(3,new SQLInteger(13));
        valueRow.setColumn(4,new SQLVarchar());
        record.setData(new int[]{2,4,5,6},valueRow);

        ValueRow valueRow2 = new ValueRow(4);
        valueRow2.setColumn(1,new SQLVarchar("FOOEY"));
        valueRow2.setColumn(2,new SQLInteger(123));
        valueRow2.setColumn(3,new SQLInteger());
        valueRow2.setColumn(4,new SQLVarchar());
        record2.setData(new int[]{2,3,5,6},valueRow2);

        ValueRow rowDefinition = new ValueRow(8);
        rowDefinition.setColumn(1,new SQLInteger());
        rowDefinition.setColumn(2,new SQLInteger());
        rowDefinition.setColumn(3,new SQLVarchar());
        rowDefinition.setColumn(4,new SQLInteger());
        rowDefinition.setColumn(5,new SQLInteger());
        rowDefinition.setColumn(6,new SQLInteger());
        rowDefinition.setColumn(7,new SQLVarchar());
        rowDefinition.setColumn(8,new SQLInteger());
        Record[] records = record.updateRecord(record2,rowDefinition);
        Assert.assertEquals("");
        System.out.println(records[0].getData(IntArrays.count(8),rowDefinition));

    }

}