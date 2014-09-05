package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.NumberDataValue;
import org.junit.Test;

import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.impl.sql.execute.operations.window.function.SumAggregator;

/**
 * @author Jeff Cunningham
 *         Date: 8/21/14
 */
public class SumWindowFunctionTest extends WindowTestingFramework {

    @Test
    public void testSumBufferingDefaultFrame() throws Exception {
        //
        // configure the test
        //
        // create and setup the function
        SumAggregator function = new SumAggregator();
        function.setup(cf, "sum", DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.BIGINT, false));

        // create the number of partitions, number of rows in partition, set (1-based) partition and orderby column IDs
        int nPartitions = 5;
        int partitionSize = 3;
        int[] partitionColIDs = new int[0];  // 1-based
        int[] orderByColIDs = new int[0];    // 1-based
        int[] inputColIDs = new int[] {2};   // 1-based

        // define the shape of the input rows
        List<TestColumnDefinition> rowDefinition = new ArrayList<TestColumnDefinition>(
            Arrays.asList(new TestColumnDefinition[]{
                new IntegerColumnDefinition(),
                new DoubleColumnDefinition().setVariant(13)}));

        // create frame definition and frame buffer we'll use
        FrameDefinition frameDefinition = DEFAULT_FRAME_DEF;

        // create the function that will generate expected results
        ExpectedResultsFunction expectedResultsFunction = new SumFunct(2);


        // test the config
        helpTestWindowFunction(nPartitions, partitionSize, partitionColIDs, orderByColIDs, inputColIDs, rowDefinition,
                               frameDefinition,
                               expectedResultsFunction, function, DONT_PRINT_RESULTS);
    }

    //===============================================================================================
    // helpers
    //===============================================================================================

    private void helpTestColumns(int[] partitionColIDs, int[] orderByColIDs, boolean print)
        throws IOException, StandardException {
        SumAggregator function = new SumAggregator();
        function.setup(cf, "sum", DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.BIGINT, false));

        int nPartitions = 5;
        int partitionSize = 50;
        ExpectedResultsFunction expectedResultsFunction = new SumFunct(2);

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
     * Implementation of dense rank function used to generate expected results from given input rows.
     */
    private static class SumFunct implements ExpectedResultsFunction {
        private final int aggIndex;
        private NumberDataValue last;

        protected SumFunct(int aggColID) {
            // convert to 0-based
            this.aggIndex = aggColID-1;
        }

        @Override
        public void reset() {
            last = null;
        }

        @Override
        public DataValueDescriptor apply(DataValueDescriptor[] input) throws StandardException {
            NumberDataValue inputValue = (NumberDataValue)input[aggIndex].cloneValue(false);
            NumberDataValue result = null;
            if (last == null) {
                last = inputValue;
                result = (NumberDataValue) last.cloneValue(false);
            } else {
                result = inputValue.plus(last, inputValue, result);
                last = result;
            }
            return result;
        }

        @Override
        public DataValueDescriptor getNullReturnValue() throws StandardException {
            return LazyDataValueFactory.getLazyNull(StoredFormatIds.SQL_LONGINT_ID);
        }
    }
}
