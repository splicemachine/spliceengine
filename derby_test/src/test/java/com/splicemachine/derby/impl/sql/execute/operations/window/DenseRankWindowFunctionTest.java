package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.junit.Assert;
import org.junit.Test;

import com.splicemachine.derby.impl.sql.execute.operations.window.function.DenseRankFunction;

/**
 * @author Jeff Cunningham
 *         Date: 8/21/14
 */
public class DenseRankWindowFunctionTest extends WindowTestingFramework {

    @Test
    public void testDenseRankBufferingDefaultFrame() throws Exception {
        //
        // configure the test
        //
        // create and setup the function
        DenseRankFunction function = new DenseRankFunction();
        function.setup(cf, "denseRank", DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.BIGINT, false));

        // create the number of partitions, number of rows in partition, set (1-based) partition and orderby column IDs
        int nPartitions = 5;
        int partitionSize = 50;
        int[] partitionColIDs = new int[] {1};  // 1-based
        int[] orderByColIDs = new int[] {2};    // 1-based

        // define the shape of the input rows
        List<TestColumnDefinition> rowDefinition = new ArrayList<TestColumnDefinition>(
            Arrays.asList(new TestColumnDefinition[]{
                new IntegerColumnDefinition(),
                new DoubleColumnDefinition().setVariant(13),
                new VarcharColumnDefinition(7).setVariant(5),
                new TimestampColumnDefinition().setVariant(9),
                new DateColumnDefinition().setVariant(13)}));

        // create frame definition and frame buffer we'll use
        FrameDefinition frameDefinition = DEFAULT_FRAME_DEF;

        // create the function that will generate expected results
        ExpectedResultsFunction expectedResultsFunction = new DenseRankFunct(partitionColIDs, orderByColIDs);


        // test the config
        helpTestWindowFunction(nPartitions, partitionSize, partitionColIDs, orderByColIDs, orderByColIDs, rowDefinition,
                               frameDefinition,
                               expectedResultsFunction, function, DONT_PRINT_RESULTS);
    }

    @Test
    public void testEmptyOrderByColumns() throws Exception {

        // define the shape of the input rows
        List<TestColumnDefinition> rowDefinition = new ArrayList<TestColumnDefinition>(
            Arrays.asList(new TestColumnDefinition[]{
                new IntegerColumnDefinition()}));

        try {
            helpTestColumns(5, 100, new int[]{1}, new int[0], rowDefinition, true);
            Assert.fail("Expected exception - no ranking rows.");
        } catch (IOException e) {
            // shouldn't happen
            Assert.fail(e.getMessage());
        } catch (StandardException e) {
            // shouldn't happen
            Assert.fail(e.getMessage());
        } catch (RuntimeException e) {
            //expected
        }
    }

    @Test
    public void testEmptyPartitionColumns() throws Exception {

        // define the shape of the input rows
        List<TestColumnDefinition> rowDefinition = new ArrayList<TestColumnDefinition>(
            Arrays.asList(new TestColumnDefinition[]{
                new IntegerColumnDefinition(),
                new DoubleColumnDefinition().setVariant(13)}));

        helpTestColumns(5, 100, new int[0], new int[] {2}, rowDefinition, DONT_PRINT_RESULTS);
    }

    @Test
    public void testTimestampColumn() throws Exception {

        // define the shape of the input rows
        List<TestColumnDefinition> rowDefinition = new ArrayList<TestColumnDefinition>(
            Arrays.asList(new TestColumnDefinition[]{
                new IntegerColumnDefinition(),
                new DoubleColumnDefinition().setVariant(13),
                new VarcharColumnDefinition(7).setVariant(5),
                new TimestampColumnDefinition().setVariant(9),
                new DateColumnDefinition().setVariant(13)}));
        helpTestColumns(5, 100, new int[] {1}, new int[] {4}, rowDefinition, DONT_PRINT_RESULTS);
    }

    @Test
    public void testStringColumn() throws Exception {

        // define the shape of the input rows
        List<TestColumnDefinition> rowDefinition = new ArrayList<TestColumnDefinition>(
            Arrays.asList(new TestColumnDefinition[]{
                new IntegerColumnDefinition(),
                new DoubleColumnDefinition().setVariant(13),
                new VarcharColumnDefinition(7).setVariant(5),
                new TimestampColumnDefinition().setVariant(9),
                new DateColumnDefinition().setVariant(13)}));
        helpTestColumns(5, 100, new int[] {1}, new int[] {3}, rowDefinition, DONT_PRINT_RESULTS);
    }

    @Test
    public void testThreeOrderByColumns() throws Exception {

        // define the shape of the input rows
        List<TestColumnDefinition> rowDefinition = new ArrayList<TestColumnDefinition>(
            Arrays.asList(new TestColumnDefinition[]{
                new IntegerColumnDefinition(),
                new DoubleColumnDefinition().setVariant(13).setColumnName("partition_and_input1"),
                new VarcharColumnDefinition(7).setVariant(5).setColumnName("input2"),
                new TimestampColumnDefinition().setVariant(9).setColumnName("input3"),
                new DateColumnDefinition().setVariant(13)}));

        printInputSet(rowDefinition, 5, new int[] {2,3,4}, "temp1", System.out);

        helpTestColumns(5, 100, new int[] {2}, new int[] {2,3,4}, rowDefinition, DONT_PRINT_RESULTS);
    }

    //===============================================================================================
    // helpers
    //===============================================================================================

    private void helpTestColumns(int nPartitions, int partitionSize, int[] partitionColIDs, int[] orderByColIDs, List<TestColumnDefinition> rowDefinition, boolean print)
        throws IOException, StandardException {
        DenseRankFunction function = new DenseRankFunction();
        function.setup(cf, "denseRank", DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.BIGINT, false));

        ExpectedResultsFunction expectedResultsFunction = new DenseRankFunct(partitionColIDs, orderByColIDs);

        // test the config
        helpTestWindowFunction(nPartitions, partitionSize, partitionColIDs, orderByColIDs, orderByColIDs, rowDefinition,
                               DEFAULT_FRAME_DEF, expectedResultsFunction, function, print);
    }

    /**
     * Implementation of dense rank function used to generate expected results from given input rows.
     */
    private static class DenseRankFunct extends WindowTestingFramework.RankingFunct implements ExpectedResultsFunction {

        /**
         * Init with the identifiers of the partition and orderby columns.
         * @param partitionColIDs the 1-based partition column identifiers. Can be empty.
         * @param orderByColIDs the 1-based orderby column identifiers. Can be empty.
         */
        public DenseRankFunct(int[] partitionColIDs, int[] orderByColIDs) {
            super(partitionColIDs, orderByColIDs);
        }

        @Override
        public void reset() {
            lastRow = null;
            rowNum = 0;
        }

        @Override
        public DataValueDescriptor apply(DataValueDescriptor[] input) throws StandardException {
            if (dvdArraysDiffer(lastRow, input, partitionColIDs)) {
                // new partition, dense rank increases
                reset();
                ++rowNum;
            } else if (dvdArraysDiffer(lastRow, input, orderByColIDs)) {
                lastRow = input;
                ++rowNum;
            }
            lastRow = input;
            return WindowTestingFramework.dvf.getDataValue(rowNum, null);
        }
    }
}
