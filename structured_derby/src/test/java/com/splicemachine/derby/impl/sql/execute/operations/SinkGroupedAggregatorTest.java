package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.GroupedRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.GroupedAggregateBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.ScanGroupedAggregateIterator;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.SinkGroupedAggregateIterator;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.derby.utils.StandardSupplier;

import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.stats.Metrics;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.UserType;
import org.apache.derby.impl.sql.execute.AggregatorInfo;
import org.apache.derby.impl.sql.execute.CountAggregator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 *         Created on: 11/5/13
 */
public class SinkGroupedAggregatorTest {

    @Test
    public void testCanAggregateDistinctAndNonDistinctTogether() throws Exception {
        int size =10;
        List<ExecRow> sourceRows = Lists.newArrayListWithCapacity(size);
        final ExecRow template = new ValueRow(7);
        template.setRowArray(new DataValueDescriptor[]{
                new SQLInteger(),
                new SQLInteger(),
                new SQLInteger(),
                new UserType(),
                new SQLInteger(),
                new SQLInteger(),
                new UserType()
        });
        for(int i=0;i<size;i++){
            template.resetRowArray();
            template.getColumn(1).setValue(i%3); //group into three grouping fields
            template.getColumn(2).setValue(i%3); //group into three grouping fields
            template.getColumn(5).setValue(i%2); //add a distinct column
            sourceRows.add(template.getClone());
        }

        /* SourceRows:
         * 0, 0, NULL, NULL, 0, NULL, NULL
         * 1, 1, NULL, NULL, 1, NULL, NULL
         * 2, 2, NULL, NULL, 0, NULL, NULL
         * 0, 0, NULL, NULL, 1, NULL, NULL
         * 1, 1, NULL, NULL, 0, NULL, NULL
         * 2, 2, NULL, NULL, 1, NULL, NULL
         * 0, 0, NULL, NULL, 0, NULL, NULL
         * 1, 1, NULL, NULL, 1, NULL, NULL
         * 2, 2, NULL, NULL, 0, NULL, NULL
         * 0, 0, NULL, NULL, 1, NULL, NULL
         */

        StandardIterator<ExecRow> source = StandardIterators.wrap(sourceRows);

        StandardSupplier<ExecRow> emptyRowSupplier = new StandardSupplier<ExecRow>() {
            @Override
            public ExecRow get() throws StandardException {
                return template.getClone();
            }
        };
        WarningCollector collector = new WarningCollector() {
            @Override
            public void addWarning(String warningState) throws StandardException {
                Assert.fail("Should not emit warnings!");
            }
        };
        SpliceGenericAggregator nonDistinctAggregate = getCountAggregator(4,2,3,false);
        SpliceGenericAggregator distinctAggregate = getCountAggregator(7,5,6,true);
        GroupedAggregateBuffer nonDistinctBuffer = new GroupedAggregateBuffer(10,
                new SpliceGenericAggregator[]{nonDistinctAggregate},false,emptyRowSupplier,collector, Metrics.noOpMetricFactory(), true);
        GroupedAggregateBuffer distinctBuffer = new GroupedAggregateBuffer(10,
                new SpliceGenericAggregator[]{distinctAggregate},false,emptyRowSupplier,collector, Metrics.noOpMetricFactory(), true);

        int[] groupColumns = new int[]{0};
        boolean[] groupSortOrder = new boolean[]{true};
        int[] uniqueNonGroupedColumns = new int[]{4};

				DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(template);
        SinkGroupedAggregateIterator aggregator = SinkGroupedAggregateIterator.newInstance(nonDistinctBuffer,
								distinctBuffer,
                source,false,groupColumns,groupSortOrder,uniqueNonGroupedColumns,serializers);

        //1 row for each nonDistinctAggregate * unique groupings = 3 * 1 = 3
        List<GroupedRow> nonDistinctResults = Lists.newArrayListWithExpectedSize(3);
        //1 row for each (grouping, unique key) pair = 3 for each grouping pair + 4 for one = 9+4 =  13
        List<GroupedRow> distinctResults = Lists.newArrayListWithExpectedSize(size/3 + size%3);
        Set<byte[]> nonDistinctValues = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
        List<GroupedRow> allRows = Lists.newArrayList();
        GroupedRow row = aggregator.next(null);
        while(row!=null){
            if(row.isDistinct()){
                System.out.println(String.format("Adding row %s to distinct", row));
                distinctResults.add(row.deepCopy());
            }else{
                Assert.assertFalse("Duplicate grouping key seen!",nonDistinctValues.contains(row.getGroupingKey()));
                nonDistinctValues.add(row.getGroupingKey());
                nonDistinctResults.add(row.deepCopy());
            }
            allRows.add(row.deepCopy());
            row = aggregator.next(null);
        }

        Assert.assertEquals("Incorrect nonDistinctSize results",size/3,nonDistinctResults.size());
        Assert.assertEquals("Incorrect distinctSize results", 2 * nonDistinctResults.size(), distinctResults.size());

        StandardIterator<ExecRow> scanSource = StandardIterators.wrap(Lists.transform(allRows,new Function<GroupedRow, ExecRow>() {
            @Override
            public ExecRow apply(@Nullable GroupedRow input) {
                //noinspection ConstantConditions
                return input.getRow();
            }
        }));

        GroupedAggregateBuffer scanBuffer = new GroupedAggregateBuffer(10,
                new SpliceGenericAggregator[]{nonDistinctAggregate,distinctAggregate},false,emptyRowSupplier,collector,true, Metrics.noOpMetricFactory(), true);

				KeyEncoder encoder = KeyEncoder.bare(groupColumns,groupSortOrder,serializers);
        ScanGroupedAggregateIterator scanAggregator = new ScanGroupedAggregateIterator(scanBuffer,scanSource,encoder,groupColumns,false);

        List<GroupedRow> scanRows = Lists.newArrayListWithExpectedSize(3);
        row = scanAggregator.next(null);
        while(row!=null){
            scanRows.add(row.deepCopy());
            row = scanAggregator.next(null);
        }
        Assert.assertEquals("incorrect size!",size/3,scanRows.size());

    }

    private SpliceGenericAggregator getCountAggregator(int aggregatorColumnId,
                                                       int inputColumn,
                                                       int resultColumnId,boolean distinct) {
        CountAggregator execAggregator = new CountAggregator();
        execAggregator.setup(null, "COUNT(*)", null);
        SpliceGenericAggregator aggregator = new SpliceGenericAggregator(execAggregator,aggregatorColumnId,inputColumn,resultColumnId);
        AggregatorInfo mockInfo = mock(AggregatorInfo.class);
        when(mockInfo.isDistinct()).thenReturn(distinct);
        when(mockInfo.getInputColNum()).thenReturn(inputColumn - 1);
        aggregator.setAggInfo(mockInfo);
        return aggregator;
    }
}
