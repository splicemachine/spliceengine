package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.junit.Test;

import com.splicemachine.derby.impl.sql.execute.operations.window.function.RowNumberFunction;

/**
 * @author Jeff Cunningham
 *         Date: 8/21/14
 */
public class RowNumberWindowFunctionTest extends WindowTestingFramework {

    @Test
    public void testRowNumberBufferingDefaultFrame() throws Exception {
        //
        // configure the test
        //
        // create and setup the function
        RowNumberFunction function = new RowNumberFunction();
        function.setup(cf, "row_number", DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.BIGINT, false));

        // create the number of partitions, number of rows in partition, set (1-based) partition and orderby column IDs
        int nPartitions = 5;
        int partitionSize = 50;
        int[] partitionColIDs = new int[] {1};
        int[] orderByColIDs = new int[] {2};

        // create frame definition and frame buffer we'll use
        FrameDefinition frameDefinition = DEFAULT_FRAME_DEF;

        // create the function that will generate expected results
        ExpectedResultsFunction expectedResultsFunction = new RowNumberFunct(partitionColIDs, orderByColIDs);

        // define the shape of the input rows
        List<TestColumnDefinition> rowDefinition = new ArrayList<TestColumnDefinition>(
            Arrays.asList(new TestColumnDefinition[]{
                new IntegerColumnDefinition(),
                new DoubleColumnDefinition().setVariant(13),
                new VarcharColumnDefinition(7).setVariant(5),
                new TimestampColumnDefinition().setVariant(9),
                new DateColumnDefinition().setVariant(13)}));

        // test the config
        helpTestWindowFunction(nPartitions, partitionSize, partitionColIDs, orderByColIDs, orderByColIDs, rowDefinition, frameDefinition,
                               expectedResultsFunction, function, DONT_PRINT_RESULTS);
    }

    @Test
    public void testStringColumn() throws Exception {
        helpTestColumns(new int[] {1}, new int[] {3}, DONT_PRINT_RESULTS);
    }

    @Test
    public void testTimestampColumn() throws Exception {
        helpTestColumns(new int[] {1}, new int[] {4}, DONT_PRINT_RESULTS);
    }

    @Test
    public void testThreeOrderByColumns() throws Exception {
        helpTestColumns(new int[] {2}, new int[] {2,3,4}, DONT_PRINT_RESULTS);
    }

    //===============================================================================================
    // helpers
    //===============================================================================================

    private void helpTestColumns(int[] partitionColIDs, int[] orderByColIDs, boolean print)
        throws IOException, StandardException {
        RowNumberFunction function = new RowNumberFunction();
        function.setup(cf, "function", DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.BIGINT, false));

        int nPartitions = 5;
        int partitionSize = 50;
        ExpectedResultsFunction expectedResultsFunction = new RowNumberFunct(partitionColIDs, orderByColIDs);

        // define the shape of the input rows
        List<TestColumnDefinition> rowDefinition = new ArrayList<TestColumnDefinition>(
            Arrays.asList(new TestColumnDefinition[]{
                new IntegerColumnDefinition(),
                new DoubleColumnDefinition().setVariant(13),
                new VarcharColumnDefinition(7).setVariant(5),
                new TimestampColumnDefinition().setVariant(9),
                new DateColumnDefinition().setVariant(13)}));

        // test the config
        helpTestWindowFunction(nPartitions, partitionSize, partitionColIDs, orderByColIDs,
                               orderByColIDs, rowDefinition, DEFAULT_FRAME_DEF,
                               expectedResultsFunction, function, print);
    }

    /**
     * Implementation of row number function used to generate expected results from given input rows.
     */
    private static class RowNumberFunct extends WindowTestingFramework.RankingFunct implements ExpectedResultsFunction {

        /**
         * Init with the identifiers of the partition and orderby columns.
         * @param partitionColIDs the 1-based partition column identifiers. Can be empty.
         * @param orderByColIDs the 1-based orderby column identifiers. Can be empty.
         */
        public RowNumberFunct(int[] partitionColIDs, int[] orderByColIDs) {
            super(partitionColIDs, orderByColIDs);
        }

        @Override
        public void reset() {
            rowNum = 0;
        }

        @Override
        public DataValueDescriptor apply(DataValueDescriptor[] input) throws StandardException {
            if (dvdArraysDiffer(lastRow, input, partitionColIDs)) {
                reset();
            }
            lastRow = input;
            // row number always increases w/i a partition
            return WindowTestingFramework.dvf.getDataValue(++rowNum, null);
        }
    }
}
