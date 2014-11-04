package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import com.splicemachine.encoding.Encoding;
import com.splicemachine.metrics.Metrics;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.impl.sql.execute.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

import java.util.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * @author P Trolard
 *         Date: 24/06/2014
 */
public class GroupedAggregatorTest {

    /**
     * Perform aggregation given by aggregators over sourceRows, simulating the sinking
     * and scanning phases of the GroupedAggregateOperation. Returned map contains the
     * following lists of GroupedRow results:
     *
     * sunkNonDistinctRows: rows sunk to TEMP from the non-distinct buffer
     * sunkDistinctRows: rows sunk to TEMP from the distinct buffer
     * allSunkRows: all rows sunk to TEMP (union of above two)
     * allScannedRows: all rows scanned from TEMP then processed by the scan-side buffer
     *
     */
    public static Map<String, List<GroupedRow>> runGroupedAggregateTest(final List<ExecRow> sourceRows,
                                                                        SpliceGenericAggregator... aggregators)
        throws Exception {

        // Stores results callers may want to inspect
        final Map<String,List<GroupedRow>> results = Maps.newHashMap();


        // Categorize aggregators
        List<SpliceGenericAggregator> nonDistinctAggs = Lists.newLinkedList();
        List<SpliceGenericAggregator> distinctAggs = Lists.newLinkedList();

        for (SpliceGenericAggregator agg: aggregators){
            if (agg.isDistinct()){
                distinctAggs.add(agg);
            } else {
                nonDistinctAggs.add(agg);
            }
        }
        int[] uniqueNonGroupedCols = distinctAggs.size() > 0 ? new int[distinctAggs.size()]
                                         : new int[0];
        if (distinctAggs.size() > 0){
            for (int i = 0; i < distinctAggs.size(); i++) {
                uniqueNonGroupedCols[i] =
                    distinctAggs.get(i).getAggregatorInfo().getInputColNum();
            }
        }
        // groupColumns: assume the first
        int[] groupColumns = new int[]{0};
        boolean[] groupSortOrder = new boolean[]{true};


        // Create buffers & accessories for sinking to TEMP

        final ExecRow template = sourceRows.get(0).getClone();
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

        GroupedAggregateBuffer nonDistinctBuffer =
            new GroupedAggregateBuffer(10,
                                          nonDistinctAggs.toArray(new SpliceGenericAggregator[nonDistinctAggs.size()]),
                                          false, emptyRowSupplier, collector, Metrics.noOpMetricFactory(), true);
        GroupedAggregateBuffer distinctBuffer =
            new GroupedAggregateBuffer(10, distinctAggs.toArray(new SpliceGenericAggregator[distinctAggs.size()]),
                                          false, emptyRowSupplier, collector, Metrics.noOpMetricFactory(), true);

        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(template);

        SinkGroupedAggregateIterator aggregator = SinkGroupedAggregateIterator
                                                      .newInstance(nonDistinctBuffer,
                                                                      distinctBuffer,
                                                                      source,
                                                                      false,
                                                                      groupColumns,
                                                                      groupSortOrder,
                                                                      uniqueNonGroupedCols,
                                                                      serializers);

        results.put("sunkDistinctRows",     new LinkedList<GroupedRow>());
        results.put("sunkNonDistinctRows",  new LinkedList<GroupedRow>());
        results.put("allSunkRows",          new LinkedList<GroupedRow>());

        // Produce rows that would be sunk to TEMP, categorizing by distinct or not
        GroupedRow row = aggregator.next(null);
        while (row != null){
            if (row.isDistinct()){
                results.get("sunkDistinctRows").add(row.deepCopy());
            } else {
                results.get("sunkNonDistinctRows").add(row.deepCopy());
            }
            results.get("allSunkRows").add(row.deepCopy());
            row = aggregator.next(null);
        }

        // Sort rows to simulate ordering from TEMP, making sure that for a given key
        // rows produced by the distinct buffer are read first, to test for bug triggered
        // when that's the case. (TEMP won't always order rows this way, but when it
        // happens to do so it was causing DB-1277.)
        List<GroupedRow> sortedAllRows = sortedByKeyThenDistinct(results.get("allSunkRows"));

        StandardIterator<ExecRow> scanSource =
            StandardIterators.wrap(Lists.transform(sortedAllRows, execFromGrouped));

        GroupedAggregateBuffer scanBuffer = new GroupedAggregateBuffer(10,
                                                                          aggregators,
                                                                          false,
                                                                          emptyRowSupplier,
                                                                          collector,
                                                                          true,
                                                                          Metrics.noOpMetricFactory(),
                                                                          true);

        // Read rows as if from TEMP & perform aggregation
        ScanGroupedAggregateIterator scanAggregator =
            new ScanGroupedAggregateIterator(scanBuffer,
                                                scanSource,
                                                KeyEncoder.bare(groupColumns, groupSortOrder, serializers),
                                                groupColumns,
                                                false);

        results.put("allScannedRows", new LinkedList<GroupedRow>());
        row = scanAggregator.next(null);
        while (row != null) {
            results.get("allScannedRows").add(row.deepCopy());
            row = scanAggregator.next(null);
        }

        return results;

    }

     /*
     * Returns a new list of GroupedRows sorted first by grouping key then by
     * whether it's distinct, where distinct rows sort before non-distinct rows
     */
    private static List<GroupedRow> sortedByKeyThenDistinct(List<GroupedRow> rows) {
        List<GroupedRow> sorted = Lists.newArrayList(rows);
        Collections.sort(sorted, new Comparator<GroupedRow>() {
            @Override
            public int compare(GroupedRow a, GroupedRow b) {
                int keyComp = Bytes.compareTo(a.getGroupingKey(),
                                                 b.getGroupingKey());
                if (keyComp != 0) {
                    return keyComp;
                } else {
                    if (a.isDistinct() && b.isDistinct()) {
                        return 0;
                    } else if (a.isDistinct()) {
                        return -1;
                    } else {
                        return 1;
                    }
                }
            }
        });
        return sorted;
    }

    public static Function<GroupedRow,ExecRow> execFromGrouped = new Function<GroupedRow, ExecRow>() {
        @Override
        public ExecRow apply(GroupedRow r) {
            return r.getRow();
        }
    };

    public static Function<GroupedRow,byte[]> keyFromGrouped = new Function<GroupedRow, byte[]>() {
        @Override
        public byte[] apply(GroupedRow r) {
            return r.getGroupingKey();
        }
    };

    public static void assertGroupingKeysAreUnique(List<GroupedRow> rows){
         Set<byte[]> distinctKeys =
            ImmutableSortedSet
                .orderedBy(Bytes.BYTES_COMPARATOR)
                .addAll(Lists.transform(rows, keyFromGrouped))
                .build();
        Assert.assertTrue("Duplicate grouping key seen!", rows.size() == distinctKeys.size());
    }

    public static Map<Integer, List<Object>> groupingKeyToResults(List<GroupedRow> results,
                                                                  SpliceGenericAggregator... aggs)
        throws StandardException {
        int[] outputCols = new int[aggs.length];
        for (int i = 0; i < aggs.length; i++) {
            outputCols[i] = aggs[i].getAggregatorInfo().getOutputColNum() + 1;
        }

        Map<Integer, List<Object>> keyToResultCols = Maps.newHashMap();
        for (GroupedRow r : results) {
            List<Object> resultCols = Lists.newLinkedList();
            for (int i : outputCols) {
                resultCols.add(r.getRow().getColumn(i).getObject());
            }
            keyToResultCols.put(Encoding.decodeInt(r.getGroupingKey()), resultCols);
        }

        return keyToResultCols;
    }

    public static <T> List<T> l(T... items) {
        return Arrays.asList(items);
    }

    public static SpliceGenericAggregator countAggregator(int aggregatorColumnId,
                                                          int inputColumn,
                                                          int resultColumnId,
                                                          boolean distinct) {
        CountAggregator execAggregator = new CountAggregator();
        execAggregator.setup(null, "COUNT(*)", null);
        SpliceGenericAggregator aggregator = new SpliceGenericAggregator(execAggregator,aggregatorColumnId,inputColumn,resultColumnId);
        aggregator.setAggInfo(mockInfo(aggregatorColumnId, inputColumn, resultColumnId, distinct));
        return aggregator;
    }

    public static SpliceGenericAggregator sumAggregator(int aggregatorColumnId,
                                                        int inputColumn,
                                                        int resultColumnId,
                                                        boolean distinct) {
        SumAggregator execAggregator = new LongBufferedSumAggregator(5);
        SpliceGenericAggregator aggregator = new SpliceGenericAggregator(execAggregator,
                                                                            aggregatorColumnId,
                                                                            inputColumn,
                                                                            resultColumnId);
        aggregator.setAggInfo(mockInfo(aggregatorColumnId, inputColumn, resultColumnId, distinct));
        return aggregator;
    }

    public static SpliceGenericAggregator avgAggregator(int aggregatorColumnId,
                                                        int inputColumn,
                                                        int resultColumnId,
                                                        boolean distinct) {
        AvgAggregator execAggregator = new AvgAggregator();
        execAggregator.setup(null, null, DataTypeDescriptor.DOUBLE);
        SpliceGenericAggregator aggregator = new SpliceGenericAggregator(execAggregator,
                                                                            aggregatorColumnId,
                                                                            inputColumn,
                                                                            resultColumnId);
        aggregator.setAggInfo(mockInfo(aggregatorColumnId, inputColumn, resultColumnId, distinct));
        return aggregator;
    }

    private static AggregatorInfo mockInfo(int aggregatorColumnId,
                                    int inputColumn,
                                    int resultColumnId,
                                    boolean distinct) {
        AggregatorInfo info = mock(AggregatorInfo.class);
        when(info.isDistinct()).thenReturn(distinct);
        when(info.getInputColNum()).thenReturn(inputColumn - 1);
        when(info.getOutputColNum()).thenReturn(resultColumnId - 1);
        when(info.getAggregatorColNum()).thenReturn(aggregatorColumnId - 1);
        return info;
    }
}
