package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Jeff Cunningham
 *         Date: 8/21/14
 */
public class RankWindowFunctionTest extends WindowTestingFramework {

    @Test
    public void testRankBufferingDefaultFrame() throws Exception {
        //
        // configure the test
        //
        // create and setup the function
        RankFunction function = new RankFunction();
        function.setup(cf, "rank", DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.BIGINT, false));

        // create the number of partitions, number of rows in partition, set (1-based) partition and orderby column IDs
        int nPartitions = 5;
        int partitionSize = 50;
        int[] partitionColIDs = new int[] {1};
        int[] orderByColIDs = new int[] {2};

        // define the shape of the input rows
        List<TestColumnDefinition> rowDefinition = new ArrayList<TestColumnDefinition>(
            Arrays.asList(new TestColumnDefinition[] {
                new IntegerColumnDefinition(),
                new DoubleColumnDefinition().setVariantColumn(true),
                new VarcharColumnDefinition(7).setVariantColumn(true),
                new TimestampColumnDefinition().setVariantColumn(true),
                new DateColumnDefinition().setVariantColumn(true)}));

        // create frame definition and frame buffer we'll use
        FrameDefinition frameDefinition = DEFAULT_FRAME_DEF;

        // create the function that will generate expected results
        ExpectedResultsFunction expectedResultsFunction = new RankFunct(partitionColIDs, orderByColIDs);

        // test the config
        helpTestWindowFunction(nPartitions, partitionSize, partitionColIDs, orderByColIDs, orderByColIDs, rowDefinition, frameDefinition,
                               expectedResultsFunction, function, DONT_PRINT_RESULTS);
    }

    @Test
    @Ignore("DB-1682 - timestamp error")
    public void testTimestampColumn() throws Exception {
        helpTestColumns(new int[] {1}, new int[] {3}, PRINT_RESULTS);
    }

    @Test
    @Ignore("DB-1645 - restricted to only one order by column")
    public void testThreeOrderByColumns() throws Exception {
        helpTestColumns(new int[] {2}, new int[] {2,3,4}, PRINT_RESULTS);
    }

    //===============================================================================================
    // helpers
    //===============================================================================================

    private void helpTestColumns(int[] partitionColIDs, int[] orderByColIDs, boolean print)
        throws IOException, StandardException {
        RankFunction function = new RankFunction();
        function.setup(cf, "function", DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.BIGINT, false));

        int nPartitions = 5;
        int partitionSize = 50;
        ExpectedResultsFunction expectedResultsFunction = new RankFunct(partitionColIDs, orderByColIDs);

        // define the shape of the input rows
        List<TestColumnDefinition> rowDefinition = new ArrayList<TestColumnDefinition>(
            Arrays.asList(new TestColumnDefinition[]{
                new IntegerColumnDefinition(),
                new DoubleColumnDefinition().setVariantColumn(true),
                new VarcharColumnDefinition(7).setVariantColumn(true),
                new TimestampColumnDefinition().setVariantColumn(true),
                new DateColumnDefinition().setVariantColumn(true)}));

        // test the config
        helpTestWindowFunction(nPartitions, partitionSize, partitionColIDs, orderByColIDs, orderByColIDs, rowDefinition, DEFAULT_FRAME_DEF,
                               expectedResultsFunction, function, print);
    }

    /**
     * Implementation of rank function used to generate expected results from given input rows.
     */
    private static class RankFunct extends WindowTestingFramework.RankingFunct implements ExpectedResultsFunction {
        private int runningCnt = 0;

        /**
         * Init with the identifiers of the partition and orderby columns.
         * @param partitionColIDs the 1-based partition column identifiers. Can be empty.
         * @param orderByColIDs the 1-based orderby column identifiers. Can be empty.
         */
        public RankFunct(int[] partitionColIDs, int[] orderByColIDs) {
            super(partitionColIDs, orderByColIDs);
        }

        @Override
        public void reset() {
            lastRow = null;
            rowNum = 0;
            runningCnt = 0;
        }

        @Override
        public DataValueDescriptor apply(DataValueDescriptor[] input) throws StandardException {
            ++runningCnt;
            if (dvdArraysDiffer(lastRow, input, partitionColIDs)) {
                // new partition, rank increases
                reset();
                ++rowNum;
                ++runningCnt;
            } else if (dvdArraysDiffer(lastRow, input, orderByColIDs)) {
                // In rank, rowNum is set to running count w/i a partition if orderBy col values differ.
                // It doesn't change if orderBy col values don't change.
                rowNum = runningCnt;
            }
            lastRow = input;
            return WindowTestingFramework.dvf.getDataValue(rowNum, null);
        }
    }
}
