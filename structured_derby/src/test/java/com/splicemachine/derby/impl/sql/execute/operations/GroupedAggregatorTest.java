package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
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
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 * Created on: 11/2/13
 */
public class GroupedAggregatorTest {


    @Test
    public void testGroupedAggregateWithRollups() throws Exception {
        List<ExecRow> sourceRows = Lists.newArrayListWithCapacity(10);
        final ExecRow template = new ValueRow(4);
        template.setRowArray(new DataValueDescriptor[]{
                new SQLInteger(),
                new SQLInteger(),
                new SQLInteger(),
                new UserType()
        });
        for(int i=0;i<10;i++){
            template.resetRowArray();
            template.getColumn(1).setValue(i);
            template.getColumn(2).setValue(i*2);
            sourceRows.add(template.getClone());
        }

        StandardIterator<ExecRow> source = StandardIterators.wrap(sourceRows);

        SpliceGenericAggregator countAggregator = getCountAggregator(4,1,3);
        AggregateBuffer buffer = new AggregateBuffer(20,
                new SpliceGenericAggregator[]{countAggregator},false,new StandardSupplier<ExecRow>() {
            @Override
            public ExecRow get() throws StandardException {
                return template.getNewNullRow();
            }
        },new WarningCollector() {
            @Override
            public void addWarning(String warningState) throws StandardException {
                Assert.fail("No warnings should be added!");
            }
        },false );

        int[] groupColumns = new int[]{0,1};
        boolean[] groupSortOrder = new boolean[]{true,true};
        GroupedAggregator aggregator = new GroupedAggregator(buffer,
                source,groupColumns,groupSortOrder,true);

        List<GroupedRow> results = Lists.newArrayListWithExpectedSize(21);
        GroupedRow row = aggregator.nextRow();
        while(row!=null){
            results.add(row.deepCopy());
            row = aggregator.nextRow();
        }

        Assert.assertEquals("Incorrect number of grouped rows returned!",21,results.size());
        Collections.sort(results,new Comparator<GroupedRow>() {
            @Override
            public int compare(GroupedRow o1, GroupedRow o2) {
                return Bytes.compareTo(o1.getGroupingKey(), o2.getGroupingKey());
            }
        });

        for(int i=0;i<results.size();i++){
            GroupedRow groupedRow = results.get(i);
            ExecRow dataRow = groupedRow.getRow();
            byte[] groupKey = groupedRow.getGroupingKey();
            MultiFieldDecoder decoder = MultiFieldDecoder.wrap(groupKey, SpliceDriver.getKryoPool());
            boolean allNull=true;
            for(int colPos=0;colPos<groupColumns.length;colPos++){
                if(decoder.nextIsNull()){
                    Assert.assertTrue("Column "+ colPos+" does not match the grouping key",dataRow.getColumn(colPos+1).isNull());
                }else{
                    allNull=false;
                    int next = decoder.decodeNextInt();
                    int actual = dataRow.getColumn(colPos+1).getInt();
                    Assert.assertEquals("incorrect grouping col",next,actual);
                }
            }
            if(allNull){
                int count = ((ExecAggregator)dataRow.getColumn(4).getObject()).getResult().getInt();
                Assert.assertEquals("Incorrect count column!",10,count);
            }else {
                int count = ((ExecAggregator)dataRow.getColumn(4).getObject()).getResult().getInt();
                Assert.assertEquals("Incorrect count column!",1,count);
            }


        }
    }
    @Test
    public void testGroupedAggregateWorksCorrectly() throws Exception {
        List<ExecRow> sourceRows = Lists.newArrayListWithCapacity(10);
        final ExecRow template = new ValueRow(3);
        template.setRowArray(new DataValueDescriptor[]{
                new SQLInteger(),
                new SQLInteger(),
                new UserType()
        });
        for(int i=0;i<10;i++){
            template.resetRowArray();
            template.getColumn(1).setValue(i);
            sourceRows.add(template.getClone());
        }

        StandardIterator<ExecRow> source = StandardIterators.wrap(sourceRows);

        SpliceGenericAggregator countAggregator = getCountAggregator(3,1,2);
        AggregateBuffer buffer = new AggregateBuffer(10,
                new SpliceGenericAggregator[]{countAggregator},false,new StandardSupplier<ExecRow>() {
            @Override
            public ExecRow get() throws StandardException {
                return template.getNewNullRow();
            }
        },new WarningCollector() {
            @Override
            public void addWarning(String warningState) throws StandardException {
                Assert.fail("No warnings should be added!");
            }
        } );

        int[] groupColumns = new int[]{0};
        boolean[] groupSortOrder = new boolean[]{true};
        GroupedAggregator aggregator = new GroupedAggregator(buffer,
                source,groupColumns,groupSortOrder,false);

        List<GroupedRow> results = Lists.newArrayListWithExpectedSize(10);
        GroupedRow row = aggregator.nextRow();
        while(row!=null){
            results.add(row.deepCopy());
            row = aggregator.nextRow();
        }

        Assert.assertEquals("Incorrect number of grouped rows returned!",10,results.size());
        Collections.sort(results,new Comparator<GroupedRow>() {
            @Override
            public int compare(GroupedRow o1, GroupedRow o2) {
                return Bytes.compareTo(o1.getGroupingKey(), o2.getGroupingKey());
            }
        });

        for(int i=0;i<10;i++){
            GroupedRow groupedRow = results.get(i);
            byte[] groupKey = groupedRow.getGroupingKey();
            Assert.assertEquals("incorrect grouping key!",i,Encoding.decodeInt(groupKey));
            ExecRow dataRow = groupedRow.getRow();
            Assert.assertEquals("Incorrect first column!",i,dataRow.getColumn(1).getInt());

            int count = ((ExecAggregator)dataRow.getColumn(3).getObject()).getResult().getInt();
            Assert.assertEquals("Incorrect count column!",1,count);
        }
    }

    private SpliceGenericAggregator getCountAggregator(int aggregatorColumnId, int inputColumn,int resultColumnId ) {
        CountAggregator execAggregator = new CountAggregator();
        execAggregator.setup("COUNT(*)");
        SpliceGenericAggregator aggregator = new SpliceGenericAggregator(execAggregator,aggregatorColumnId,inputColumn,resultColumnId);
        AggregatorInfo mockInfo = mock(AggregatorInfo.class);
        when(mockInfo.isDistinct()).thenReturn(false);
        aggregator.setAggInfo(mockInfo);
        return aggregator;
    }
}
