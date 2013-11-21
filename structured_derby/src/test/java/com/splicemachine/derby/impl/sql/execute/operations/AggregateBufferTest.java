package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.IndexValueRow;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.encoding.Encoding;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.UserType;
import org.apache.derby.impl.sql.execute.AggregatorInfo;
import org.apache.derby.impl.sql.execute.CountAggregator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * @author Scott Fines
 * Created on: 11/1/13
 */
public class AggregateBufferTest {

    @Test
    public void testEvictWhenSizeExceedsMax() throws Exception {
        ExecAggregator execAggregator = new CountAggregator();
        SpliceGenericAggregator aggregator = new SpliceGenericAggregator(execAggregator,3,1,2);
        AggregatorInfo mockInfo = mock(AggregatorInfo.class);
        when(mockInfo.isDistinct()).thenReturn(false);
        aggregator.setAggInfo(mockInfo);

        final ExecRow emptyRow = new IndexValueRow(new ValueRow(3));
        AggregateBuffer buffer = new AggregateBuffer(2,new SpliceGenericAggregator[]{aggregator},false,new StandardSupplier<ExecRow>() {
            @Override
            public ExecRow get() throws StandardException {
                return emptyRow;
            }
        }, new WarningCollector() {
            @Override
            public void addWarning(String warningState) throws StandardException {
                Assert.fail("No warnings should be added!");
            }
        });

        ExecRow row = new ValueRow(3);
        row.setRowArray(new DataValueDescriptor[]{
                new SQLInteger(1),
                new SQLInteger(),
                new UserType()
        });
        byte[] groupingKey = Encoding.encode(1);
        GroupedRow add = buffer.add(groupingKey, row);
        Assert.assertNull("Incorrectly evicted row!",add);

        Assert.assertEquals("Incorrect size report!",1,buffer.size());

        row.resetRowArray();
        row.getColumn(1).setValue(2);
        groupingKey = Encoding.encode(2);

        add = buffer.add(groupingKey,row);
        Assert.assertNull("Incorrectly evicted row!",add);
        Assert.assertEquals("Incorrect size report!",2,buffer.size());

        row.resetRowArray();
        row.getColumn(1).setValue(3);
        groupingKey = Encoding.encode(3);

        add = buffer.add(groupingKey,row);
        Assert.assertNotNull("No Row evicted!!", add);
        Assert.assertEquals("Incorrect size report!",2,buffer.size());

        List<GroupedRow> groupedRows = Lists.newArrayListWithExpectedSize(2);
        groupedRows.add(add.deepCopy());
        while(buffer.size()>0){
            add = buffer.getFinalizedRow();
            Assert.assertNotNull("Null row returned!",add);
            groupedRows.add(add.deepCopy());
        }
        Assert.assertEquals("Incorrect number of grouped rows returned!",3,groupedRows.size());

        Collections.sort(groupedRows,new Comparator<GroupedRow>() {
            @Override
            public int compare(GroupedRow o1, GroupedRow o2) {
                return Bytes.compareTo(o1.getGroupingKey(),o2.getGroupingKey());
            }
        });

        GroupedRow first = groupedRows.remove(0);
        Assert.assertArrayEquals("Incorrect first Grouping key!",Encoding.encode(1),first.getGroupingKey());
        Assert.assertEquals("incorrect count!",1,((ExecAggregator)first.getRow().getColumn(3).getObject()).getResult().getInt());
        first = groupedRows.remove(0);
        Assert.assertArrayEquals("Incorrect second Grouping key!",Encoding.encode(2),first.getGroupingKey());
        Assert.assertEquals("incorrect count!",1,((ExecAggregator)first.getRow().getColumn(3).getObject()).getResult().getInt());
        first = groupedRows.remove(0);
        Assert.assertArrayEquals("Incorrect second Grouping key!",Encoding.encode(3),first.getGroupingKey());
        Assert.assertEquals("incorrect count!",1,((ExecAggregator)first.getRow().getColumn(3).getObject()).getResult().getInt());
    }

    @Test
    public void testTwoDifferentGroupingKeysAreNotAggregatedTogether() throws Exception {
        ExecAggregator execAggregator = new CountAggregator();
        SpliceGenericAggregator aggregator = new SpliceGenericAggregator(execAggregator,3,1,2);
        AggregatorInfo mockInfo = mock(AggregatorInfo.class);
        when(mockInfo.isDistinct()).thenReturn(false);
        aggregator.setAggInfo(mockInfo);

        final ExecRow emptyRow = new IndexValueRow(new ValueRow(3));
        AggregateBuffer buffer = new AggregateBuffer(10,new SpliceGenericAggregator[]{aggregator},false,new StandardSupplier<ExecRow>() {
            @Override
            public ExecRow get() throws StandardException {
                return emptyRow;
            }
        }, new WarningCollector() {
            @Override
            public void addWarning(String warningState) throws StandardException {
                Assert.fail("No warnings should be added!");
            }
        });

        ExecRow row = new ValueRow(3);
        row.setRowArray(new DataValueDescriptor[]{
                new SQLInteger(1),
                new SQLInteger(),
                new UserType()
        });
        ExecRow indexRow = new IndexValueRow(row);
        byte[] groupingKey = Encoding.encode(1);
        GroupedRow add = buffer.add(groupingKey, indexRow);
        Assert.assertNull("Incorrectly evicted row!",add);

        Assert.assertEquals("Incorrect size report!",1,buffer.size());

        row.resetRowArray();
        row.getColumn(1).setValue(2);
        groupingKey = Encoding.encode(2);

        add = buffer.add(groupingKey,indexRow);
        Assert.assertNull("Incorrectly evicted row!",add);
        Assert.assertEquals("Incorrect size report!",2,buffer.size());

        List<GroupedRow> groupedRows = Lists.newArrayListWithExpectedSize(2);
        while(buffer.size()>0){
            add = buffer.getFinalizedRow();
            Assert.assertNotNull("Null row returned!",add);
            groupedRows.add(add.deepCopy());
        }
        Assert.assertEquals("Incorrect number of grouped rows returned!",2,groupedRows.size());

        Collections.sort(groupedRows,new Comparator<GroupedRow>() {
            @Override
            public int compare(GroupedRow o1, GroupedRow o2) {
                return Bytes.compareTo(o1.getGroupingKey(),o2.getGroupingKey());
            }
        });

        GroupedRow first = groupedRows.remove(0);
        Assert.assertArrayEquals("Incorrect first Grouping key!", Encoding.encode(1), first.getGroupingKey());
        Assert.assertEquals("incorrect count!",1,((ExecAggregator)first.getRow().getColumn(3).getObject()).getResult().getInt());
        first = groupedRows.remove(0);
        Assert.assertArrayEquals("Incorrect second Grouping key!", Encoding.encode(2), first.getGroupingKey());
        Assert.assertEquals("incorrect count!",1,((ExecAggregator)first.getRow().getColumn(3).getObject()).getResult().getInt());
    }

    @Test
    @Ignore
    public void testAggregatesMultipleRowsWithSameGroupingKey() throws Exception {
        ExecAggregator execAggregator = new CountAggregator();
        SpliceGenericAggregator aggregator = new SpliceGenericAggregator(execAggregator,3,1,2);
        AggregatorInfo mockInfo = mock(AggregatorInfo.class);
        when(mockInfo.isDistinct()).thenReturn(false);
        aggregator.setAggInfo(mockInfo);

        final ExecRow emptyRow = new ValueRow(3);
        AggregateBuffer buffer = new AggregateBuffer(10,new SpliceGenericAggregator[]{aggregator},false,new StandardSupplier<ExecRow>() {
            @Override
            public ExecRow get() throws StandardException {
                return emptyRow;
            }
        }, new WarningCollector() {
            @Override
            public void addWarning(String warningState) throws StandardException {
                Assert.fail("No warnings should be added!");
            }
        });

        ExecRow row = new ValueRow(3);
        row.setRowArray(new DataValueDescriptor[]{
                new SQLInteger(1),
                new SQLInteger(),
                new UserType()
        });
        byte[] groupingKey = Encoding.encode(1);
        for(int i=0;i<10;i++){
            GroupedRow add = buffer.add(groupingKey, row);
            Assert.assertNull("Incorrectly evicted row!",add);

            Assert.assertEquals("Incorrect size report!",1,buffer.size());
        }
        GroupedRow groupedRow = buffer.getFinalizedRow();
        Assert.assertNotNull("Incorrectly received null row!",groupedRow);

        Assert.assertArrayEquals("Incorrect grouping key!",groupingKey,groupedRow.getGroupingKey());

        System.out.println("actual row" + row);

        System.out.println("groupedRow row" + groupedRow.getRow());
        
        assertRowsEquals("Incorrect row", row, groupedRow.getRow());
    }

    @Test
    @Ignore
    public void testAggregatesMultipleRowsWithSameGroupingKeyDistinct() throws Exception {
        ExecAggregator execAggregator = new CountAggregator();
        SpliceGenericAggregator aggregator = new SpliceGenericAggregator(execAggregator,3,1,2);
        AggregatorInfo mockInfo = mock(AggregatorInfo.class);
        when(mockInfo.isDistinct()).thenReturn(true);
        aggregator.setAggInfo(mockInfo);

        final ExecRow emptyRow = new IndexValueRow(new ValueRow(3));
        AggregateBuffer buffer = new AggregateBuffer(10,new SpliceGenericAggregator[]{aggregator},false,new StandardSupplier<ExecRow>() {
            @Override
            public ExecRow get() throws StandardException {
                return emptyRow;
            }
        }, new WarningCollector() {
            @Override
            public void addWarning(String warningState) throws StandardException {
                Assert.fail("No warnings should be added!");
            }
        });

        ExecRow row = new ValueRow(3);
        row.setRowArray(new DataValueDescriptor[]{
                new SQLInteger(1),
                new SQLInteger(),
                new UserType()
        });
        byte[] groupingKey = Encoding.encode(1);
        for(int i=0;i<10;i++){
            GroupedRow add = buffer.add(groupingKey, row);
            Assert.assertNull("Incorrectly evicted row!",add);

            Assert.assertEquals("Incorrect size report!",1,buffer.size());
        }
        GroupedRow groupedRow = buffer.getFinalizedRow();

        System.out.println("actual row" + row);
        System.out.println("groupedRow row" + groupedRow.getRow());

        
        Assert.assertNotNull("Incorrectly received null row!",groupedRow);

        Assert.assertArrayEquals("Incorrect grouping key!",groupingKey,groupedRow.getGroupingKey());
        Assert.assertEquals("incorrect count!", 1, ((ExecAggregator) groupedRow.getRow().getColumn(3).getObject()).getResult().getInt());
        assertRowsEquals("Incorrect row", row, groupedRow.getRow());
    }

    @Test
    @Ignore
    public void testAggregateBufferWorksWithNoDistincts() throws Exception {
        ExecAggregator execAggregator = new CountAggregator();
        SpliceGenericAggregator aggregator = new SpliceGenericAggregator(execAggregator,3,1,2);
        AggregatorInfo mockInfo = mock(AggregatorInfo.class);
        when(mockInfo.isDistinct()).thenReturn(false);
        aggregator.setAggInfo(mockInfo);

        final ExecRow emptyRow = new ValueRow(3);
        AggregateBuffer buffer = new AggregateBuffer(10,new SpliceGenericAggregator[]{aggregator},false,new StandardSupplier<ExecRow>() {
            @Override
            public ExecRow get() throws StandardException {
                return emptyRow;
            }
        }, new WarningCollector() {
            @Override
            public void addWarning(String warningState) throws StandardException {
                Assert.fail("No warnings should be added!");
            }
        });

        ExecRow row = new ValueRow(3);
        row.setRowArray(new DataValueDescriptor[]{
                new SQLInteger(1),
                new SQLInteger(1),
                new UserType(execAggregator)
        });
        byte[] groupingKey = Encoding.encode(1);

        GroupedRow add = buffer.add(groupingKey, row);
        Assert.assertNull("Incorrectly evicted row!",add);

        GroupedRow groupedRow = buffer.getFinalizedRow();
        Assert.assertNotNull("Incorrectly received null row!",groupedRow);

        Assert.assertArrayEquals("Incorrect grouping key!", groupingKey, groupedRow.getGroupingKey());
        //make sure that the count aggregator returns the correct count
        Assert.assertEquals("incorrect count!",1,((ExecAggregator)groupedRow.getRow().getColumn(3).getObject()).getResult().getInt());
        assertRowsEquals("Incorrect row",row,groupedRow.getRow());
    }

    private static void assertRowsEquals(String message,ExecRow expected, ExecRow actual) {
        if(expected==actual) return;

        DataValueDescriptor[] expectedDvds = expected.getRowArray();
        DataValueDescriptor[] actualDvds = actual.getRowArray();
        Assert.assertEquals("Incorrect row length",expectedDvds.length,actualDvds.length);

        for(int i=0;i<expectedDvds.length;i++){
            Assert.assertEquals(message+" at position "+ i,expectedDvds[i],actualDvds[i]);
        }
    }
}
