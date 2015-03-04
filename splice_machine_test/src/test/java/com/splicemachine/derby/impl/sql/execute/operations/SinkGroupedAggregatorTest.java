package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.*;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.*;
import com.splicemachine.derby.impl.sql.execute.operations.framework.GroupedRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.junit.Assert;
import org.junit.Test;


import java.util.*;

import static com.splicemachine.derby.impl.sql.execute.operations.GroupedAggregatorTest.l;

/**
 * @author Scott Fines
 *         Created on: 11/5/13
 */
public class SinkGroupedAggregatorTest {

    @Test
    public void testCanAggregateDistinctAndNonDistinctTogetherWithCount() throws Exception {
        List<ExecRow> intSourceRows = intSourceRows();
        int size = intSourceRows.size();

        final SpliceGenericAggregator nonDistinctAggregate = GroupedAggregatorTest.countAggregator(4, 2, 3, false);
        final SpliceGenericAggregator distinctAggregate = GroupedAggregatorTest.countAggregator(7, 5, 6, true);

        Map<String, List<GroupedRow>> results =
            GroupedAggregatorTest.runGroupedAggregateTest(intSourceRows,
                                                             nonDistinctAggregate,
                                                             distinctAggregate);

        List<GroupedRow> sunkNonDistinctRows = results.get("sunkNonDistinctRows");

        GroupedAggregatorTest.assertGroupingKeysAreUnique(sunkNonDistinctRows);
        Assert.assertEquals("Incorrect nonDistinctSize results",size/3,sunkNonDistinctRows.size());
        Assert.assertEquals("Incorrect distinctSize results", 2 * sunkNonDistinctRows.size(),
                               results.get("sunkDistinctRows").size());


        List<GroupedRow> scannedRows = results.get("allScannedRows");
        Assert.assertEquals("incorrect size!",size/3,scannedRows.size());

        Assert.assertEquals("Incorrect aggregate results",
                               ImmutableMap.of(0, l(4, 2),
                                                  1, l(3, 2),
                                                  2, l(3, 2)),
                               GroupedAggregatorTest
                                   .groupingKeyToResults(scannedRows,
                                                            nonDistinctAggregate,
                                                            distinctAggregate));

    }

    @Test
    public void testCanAggregateDistinctAndNonDistinctTogetherWithSum() throws Exception {
        List<ExecRow> intSourceRows = intSourceRows();
        int size = intSourceRows.size();

        final SpliceGenericAggregator nonDistinctAggregate = GroupedAggregatorTest.sumAggregator(4, 2, 3, false);
        final SpliceGenericAggregator distinctAggregate = GroupedAggregatorTest.sumAggregator(7, 5, 6, true);

        Map<String, List<GroupedRow>> results =
            GroupedAggregatorTest.runGroupedAggregateTest(intSourceRows,
                                                             nonDistinctAggregate,
                                                             distinctAggregate);

        List<GroupedRow> sunkNonDistinctRows = results.get("sunkNonDistinctRows");

        GroupedAggregatorTest.assertGroupingKeysAreUnique(sunkNonDistinctRows);
        Assert.assertEquals("Incorrect nonDistinctSize results",size/3,sunkNonDistinctRows.size());
        Assert.assertEquals("Incorrect distinctSize results", 2 * sunkNonDistinctRows.size(),
                               results.get("sunkDistinctRows").size());


        List<GroupedRow> scannedRows = results.get("allScannedRows");
        Assert.assertEquals("incorrect size!",size/3,scannedRows.size());

        Assert.assertEquals("Incorrect aggregate results",
                               ImmutableMap.of(0, l(0, 1),
                                                  1, l(3, 1),
                                                  2, l(6, 1)),
                               GroupedAggregatorTest
                                .groupingKeyToResults(scannedRows,
                                                         nonDistinctAggregate,
                                                         distinctAggregate));
    }

    @Test
    public void testCanAggregateDistinctAndNonDistinctTogetherWithAvg() throws Exception {
        List<ExecRow> doubleSourceRows = doubleSourceRows();
        int size = doubleSourceRows.size();

        final SpliceGenericAggregator nonDistinctAggregate = GroupedAggregatorTest.avgAggregator(4, 2, 3, false);
        final SpliceGenericAggregator distinctAggregate = GroupedAggregatorTest.avgAggregator(7, 5, 6, true);

        Map<String, List<GroupedRow>> results =
            GroupedAggregatorTest.runGroupedAggregateTest(doubleSourceRows,
                                                             nonDistinctAggregate,
                                                             distinctAggregate);

        List<GroupedRow> sunkNonDistinctRows = results.get("sunkNonDistinctRows");

        GroupedAggregatorTest.assertGroupingKeysAreUnique(sunkNonDistinctRows);
        Assert.assertEquals("Incorrect nonDistinctSize results", size / 3, sunkNonDistinctRows.size());
        Assert.assertEquals("Incorrect distinctSize results", 2 * sunkNonDistinctRows.size(),
                               results.get("sunkDistinctRows").size());


        List<GroupedRow> scannedRows = results.get("allScannedRows");
        Assert.assertEquals("incorrect size!",size/3,scannedRows.size());

        Assert.assertEquals("Incorrect aggregate results",
                               ImmutableMap.of(0, l(0.0, 0.5),
                                                  1, l(1.0, 0.5),
                                                  2, l(2.0, 0.5)),
                               GroupedAggregatorTest
                                   .groupingKeyToResults(scannedRows,
                                                            nonDistinctAggregate,
                                                            distinctAggregate));

    }

    @Test
    public void testCanAggregateDistinctAndNonDistinctTogether1NonDistinct2Distinct() throws Exception {
        List<ExecRow> threeAggSourceRows = threeAggSourceRows();
        int size = threeAggSourceRows.size();

        final SpliceGenericAggregator nonDistinctAggregate = GroupedAggregatorTest.avgAggregator(4, 2, 3, false);
        final SpliceGenericAggregator distinctSumAggregate = GroupedAggregatorTest.sumAggregator(7, 5, 6, true);
        final SpliceGenericAggregator distinctCountAggregate = GroupedAggregatorTest.countAggregator(10, 8, 9, true);

        Map<String, List<GroupedRow>> results =
            GroupedAggregatorTest.runGroupedAggregateTest(threeAggSourceRows,
                                                             nonDistinctAggregate,
                                                             distinctSumAggregate,
                                                             distinctCountAggregate);

        List<GroupedRow> sunkNonDistinctRows = results.get("sunkNonDistinctRows");

        GroupedAggregatorTest.assertGroupingKeysAreUnique(sunkNonDistinctRows);
        Assert.assertEquals("Incorrect nonDistinctSize results", size / 3, sunkNonDistinctRows.size());
        Assert.assertEquals("Incorrect distinctSize results", 2 * sunkNonDistinctRows.size(),
                               results.get("sunkDistinctRows").size());


        List<GroupedRow> scannedRows = results.get("allScannedRows");
        Assert.assertEquals("incorrect size!",size/3,scannedRows.size());

        Assert.assertEquals("Incorrect aggregate results",
                               ImmutableMap.of(0, l(0.0, 1, 2),
                                                  1, l(1.0, 1, 2),
                                                  2, l(2.0, 1, 2)),
                               GroupedAggregatorTest
                                   .groupingKeyToResults(scannedRows,
                                                            nonDistinctAggregate,
                                                            distinctSumAggregate,
                                                            distinctCountAggregate));

    }

    @Test
    public void testCanAggregateDistinctAndNonDistinctTogether2NonDistinct1Distinct() throws Exception {
        List<ExecRow> threeAggSourceRows = threeAggSourceRows();
        int size = threeAggSourceRows.size();

        final SpliceGenericAggregator nonDistinctAggregate = GroupedAggregatorTest.avgAggregator(4, 2, 3, false);
        final SpliceGenericAggregator distinctSumAggregate = GroupedAggregatorTest.sumAggregator(7, 5, 6, false);
        final SpliceGenericAggregator distinctCountAggregate = GroupedAggregatorTest.countAggregator(10, 8, 9, true);

        Map<String, List<GroupedRow>> results =
            GroupedAggregatorTest.runGroupedAggregateTest(threeAggSourceRows,
                                                             nonDistinctAggregate,
                                                             distinctSumAggregate,
                                                             distinctCountAggregate);

        List<GroupedRow> sunkNonDistinctRows = results.get("sunkNonDistinctRows");

        GroupedAggregatorTest.assertGroupingKeysAreUnique(sunkNonDistinctRows);
        Assert.assertEquals("Incorrect nonDistinctSize results", size / 3, sunkNonDistinctRows.size());
        Assert.assertEquals("Incorrect distinctSize results", 2 * sunkNonDistinctRows.size(),
                               results.get("sunkDistinctRows").size());


        List<GroupedRow> scannedRows = results.get("allScannedRows");
        Assert.assertEquals("incorrect size!",size/3,scannedRows.size());

        Assert.assertEquals("Incorrect aggregate results",
                               ImmutableMap.of(0, l(0.0, 2, 2),
                                                  1, l(1.0, 2, 2),
                                                  2, l(2.0, 1, 2)),
                               GroupedAggregatorTest
                                   .groupingKeyToResults(scannedRows,
                                                            nonDistinctAggregate,
                                                            distinctSumAggregate,
                                                            distinctCountAggregate));

    }

    @Test
    public void testCanAggregateDistinctAndNonDistinctTogetherWithSumAndNull() throws Exception {
        List<ExecRow> intSourceRows = intSourceRowsWithNulls();

        final SpliceGenericAggregator nonDistinctAggregate = GroupedAggregatorTest.sumAggregator(4, 2, 3, false);
        final SpliceGenericAggregator distinctAggregate = GroupedAggregatorTest.sumAggregator(7, 5, 6, true);

        Map<String, List<GroupedRow>> results =
            GroupedAggregatorTest.runGroupedAggregateTest(intSourceRows,
                                                             nonDistinctAggregate,
                                                             distinctAggregate);

        List<GroupedRow> sunkNonDistinctRows = results.get("sunkNonDistinctRows");

        GroupedAggregatorTest.assertGroupingKeysAreUnique(sunkNonDistinctRows);
        Assert.assertEquals("Incorrect nonDistinctSize results", 3, sunkNonDistinctRows.size());
        Assert.assertEquals("Incorrect distinctSize results", 5,
                               results.get("sunkDistinctRows").size());


        List<GroupedRow> scannedRows = results.get("allScannedRows");
        Assert.assertEquals("incorrect size!", 3, scannedRows.size());

        Assert.assertEquals("Incorrect aggregate results",
                               ImmutableMap.of(-1, l(null, null),
                                                  1, l(6, 6),
                                                  10, l(10, 10)),
                               GroupedAggregatorTest
                                .groupingKeyToResults(scannedRows,
                                                         nonDistinctAggregate,
                                                         distinctAggregate));
    }

    private static List<ExecRow> intSourceRows() throws StandardException {

        List<ExecRow> rows = Lists.newLinkedList();
        int size = 10;
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
        for (int i = 0; i < size; i++) {
            template.resetRowArray();
            template.getColumn(1).setValue(i % 3); //group into three grouping fields
            template.getColumn(2).setValue(i % 3); //group into three grouping fields
            template.getColumn(5).setValue(i % 2); //add a distinct column
            rows.add(template.getClone());
        }

        return rows;

        /*
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

    }

    private static List<ExecRow> intSourceRowsWithNulls() throws StandardException {

        List<List<Integer>> source = Arrays.asList(
                                                      l(-1, (Integer)null),
                                                      l(1, 1),
                                                      l(-1, (Integer)null),
                                                      l(1, 3),
                                                      l(1, 2),
                                                      l(10, 10));
        List<ExecRow> rows = Lists.newLinkedList();
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
        for (List<Integer> s: source) {
            template.resetRowArray();
            if (s.get(0) == null){
                template.getColumn(1).setToNull(); // grouping field
            } else {
                template.getColumn(1).setValue((int)s.get(0));
            }
            if (s.get(1) == null){
                template.getColumn(2).setToNull();
                template.getColumn(5).setToNull();
            } else {
                template.getColumn(2).setValue((int)s.get(1)); // value field for non-distinct agg
                template.getColumn(5).setValue((int)s.get(1)); // value field for distinct agg
            }
            rows.add(template.getClone());
        }

        return rows;

    }

    private static List<ExecRow> doubleSourceRows() throws StandardException {
        List<ExecRow> rows = Lists.newLinkedList();
        int size = 10;
        final ExecRow template = new ValueRow(7);
        template.setRowArray(new DataValueDescriptor[]{
                new SQLInteger(),
                new SQLInteger(),
                new SQLDouble(),
                new UserType(),
                new SQLInteger(),
                new SQLDouble(),
                new UserType()
        });
        for (int i = 0; i < size; i++) {
            template.resetRowArray();
            template.getColumn(1).setValue(i % 3); //group into three grouping fields
            template.getColumn(2).setValue(i % 3); //group into three grouping fields
            template.getColumn(5).setValue(i % 2); //add a distinct column
            rows.add(template.getClone());
        }
        return rows;
    }

    private static List<ExecRow> threeAggSourceRows() throws StandardException {
        List<ExecRow> rows = Lists.newLinkedList();
        int size = 10;
        final ExecRow template = new ValueRow(10);
        template.setRowArray(new DataValueDescriptor[]{
                new SQLInteger(),
                new SQLInteger(),
                new SQLDouble(),
                new UserType(),
                new SQLInteger(),
                new SQLInteger(),
                new UserType(),
                new SQLInteger(),
                new SQLInteger(),
                new UserType()
        });
        for (int i = 0; i < size; i++) {
            template.resetRowArray();
            template.getColumn(1).setValue(i % 3); //group into three grouping fields
            template.getColumn(2).setValue(i % 3); //group into three grouping fields
            template.getColumn(5).setValue(i % 2); //add a distinct column
            template.getColumn(8).setValue(i % 2); //add another distinct column
            rows.add(template.getClone());
        }
        return rows;
    }
}
