/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations.window;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.WindowFunction;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.UserDataValue;
import com.splicemachine.db.impl.sql.execute.WindowFunctionInfo;
import com.splicemachine.derby.impl.sql.execute.operations.window.function.SpliceGenericWindowFunction;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

import static com.splicemachine.db.iapi.sql.compile.AggregateDefinition.FunctionType;

/**
 * Container and caching mechanism for window functions and their information container.
 *  
 */
public class WindowAggregatorImpl implements WindowAggregator {
	final int functionColumnId;
	private final int[] inputColumnIds;
	private final int resultColumnId;
    private final String functionName;
    private final FrameDefinition frameDefinition;

    private final ClassFactory cf;
    private final String functionClassName;
    private final DataTypeDescriptor resultColumnDesc;
    private final FormatableHashtable functionSpecificArgs;

    private SpliceGenericWindowFunction cachedAggregator;
    private int[] partitionColumns;
    private int[] sortColumns;
    private int[] keyColumns;
    private boolean[] keyOrders;
    private boolean[] nullOrders;
    private ColumnOrdering[] orderings;
    private ColumnOrdering[] partition;
    private FunctionType type;

    public WindowAggregatorImpl(WindowFunctionInfo windowInfo, ClassFactory cf) {
		this.cf = cf;
        // all these are one-based
		this.functionColumnId = windowInfo.getWindowFunctionColNum();
		this.inputColumnIds = windowInfo.getInputColNums();
		this.resultColumnId = windowInfo.getOutputColNum();
        this.functionName = windowInfo.getFunctionName();
        this.type = windowInfo.getType();
        this.functionSpecificArgs = windowInfo.getFunctionSpecificArgs();
        this.functionClassName = windowInfo.getWindowFunctionClassName();
        this.resultColumnDesc = windowInfo.getResultDescription().getColumnInfo()[ 0 ].getType();

        // create the frame from passed in frame info
        frameDefinition = FrameDefinition.create(windowInfo.getFrameInfo());

        partition = windowInfo.getPartitionInfo();
        partitionColumns = new int[partition.length];
        int index=0;
        for(ColumnOrdering partCol : partition){
            // coming from Derby, these are 1-based column positions
            // convert to 0-based
            partitionColumns[index++] = partCol.getColumnId() -1;
        }

        orderings = windowInfo.getOrderByInfo();
        sortColumns = new int[orderings.length];
        index = 0;
        for(ColumnOrdering order : orderings){
            // coming from Derby, these are 1-based column positions
            // convert to 0-based
            sortColumns[index++] = order.getColumnId() -1;
        }

        ColumnOrdering[] keys = windowInfo.getKeyInfo();

        keyColumns = new int[keys.length];
        keyOrders = new boolean[keys.length];
        nullOrders = new boolean[keys.length];
        index = 0;
        for (ColumnOrdering key : keys) {
            // coming from Derby, these are 1-based column positions
            // convert to 0-based
            keyColumns[index] = key.getColumnId() -1;
            nullOrders[index] = key.getIsNullsOrderedLow();
            keyOrders[index++] = key.getIsAscending();
        }
	}

    @Override
    public String toString() {
        return "WindowAggregator{'" + functionName + '\'' +
            ", functionColumnId=" + functionColumnId +
            ", inputColumnIds=" + Arrays.toString(inputColumnIds) +
            ", resultColumnId=" + resultColumnId +
            '}';
    }

    /**
     * Constructor used for testing
     * @param cachedAggregator the function to test
     * @param functionColumnId the 1-based column ID in the template row that holds the function class name
     * @param inputColumnIds the 1-based column IDs in the exec row to accept as function arguments
     * @param resultColumnId the 1-based column ID in the exec row in which to place the function result
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public WindowAggregatorImpl(WindowFunction cachedAggregator,
                                int functionColumnId,
                                int[] inputColumnIds,
                                int resultColumnId,
                                FrameDefinition frameDefinition){
        this.cachedAggregator = (SpliceGenericWindowFunction) cachedAggregator;
        this.cf=null;

        this.functionColumnId = functionColumnId;
        this.inputColumnIds = inputColumnIds;
        this.resultColumnId = resultColumnId;
        this.functionName = cachedAggregator.toString();
        this.functionClassName = cachedAggregator.toString();
        this.frameDefinition = frameDefinition;
        this.resultColumnDesc = null;
        this.functionSpecificArgs = new FormatableHashtable();
    }

    @Override
    public void accumulate(ExecRow nextRow, ExecRow accumulatorRow) throws StandardException {
        DataValueDescriptor aggCol = accumulatorRow.getColumn(functionColumnId);
        DataValueDescriptor outputCol = accumulatorRow.getColumn(resultColumnId);
        accumulate(getInputColumns(nextRow, inputColumnIds),aggCol, outputCol);
    }

	@Override
    public void finish(ExecRow row) throws StandardException{
		DataValueDescriptor outputCol = row.getColumn(resultColumnId);
		DataValueDescriptor aggCol = row.getColumn(functionColumnId);

        WindowFunction ua = (WindowFunction)aggCol.getObject();
        if(ua ==null) ua = findOrCreateNewWindowFunction(outputCol);
		
		DataValueDescriptor result = ua.getResult();
		if(result ==null) outputCol.setToNull();
		else outputCol.setValue(result);
	}

    @Override
    public boolean initialize(ExecRow row) throws StandardException {
        UserDataValue aggColumn = (UserDataValue) row.getColumn(functionColumnId);

        WindowFunction ua = (WindowFunction) aggColumn.getObject();
        DataValueDescriptor outputCol = row.getColumn(resultColumnId);
        if (ua == null) {
            ua = findOrCreateNewWindowFunction(outputCol);
            aggColumn.setValue(ua);
            return true;
        }
        return false;
    }


    @Override
    public int getResultColumnId() {
        return resultColumnId;
    }

    @Override
    public int getFunctionColumnId() {
        return functionColumnId;
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public int[] getPartitionColumns() {
        return partitionColumns;
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public int[] getKeyColumns() {
        return keyColumns;
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public int[] getSortColumns() {
        return sortColumns;
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public boolean[] getKeyOrders() {
        return keyOrders;
    }

    @Override
    public boolean[] getNullOrders() {
        return nullOrders;
    }

    @Override
    public FrameDefinition getFrameDefinition() {
        return frameDefinition;
    }

    @Override
    public String getName() {
        return functionName;
    }

    @Override
    public FunctionType getType(){
        return type;
    }

    @Override
    public SpliceGenericWindowFunction getCachedAggregator() {
        return this.cachedAggregator;
    }

    private void accumulate(DataValueDescriptor[] inputCols,DataValueDescriptor aggCol, DataValueDescriptor outputCol)
            throws StandardException{
        WindowFunction ua = (WindowFunction)aggCol.getObject();
        if(ua == null){
            ua = findOrCreateNewWindowFunction(outputCol);
        }
        ua.accumulate(inputCols);
    }

    private SpliceGenericWindowFunction findOrCreateNewWindowFunction(DataValueDescriptor resultType) throws StandardException {
        SpliceGenericWindowFunction aggInstance = cachedAggregator;
        if (aggInstance == null){
            try{
                Class aggClass = cf.loadApplicationClass(this.functionClassName);
                WindowFunction function = (WindowFunction) aggClass.newInstance();
                function.setup(cf,
                               this.functionName,
                               this.resultColumnDesc,
                               this.functionSpecificArgs
                );
                function = function.newWindowFunction();
                cachedAggregator = (SpliceGenericWindowFunction) function;
                cachedAggregator.setResultType(resultType);
                aggInstance = cachedAggregator;
            } catch(Exception e){
                throw StandardException.unexpectedUserException(e);
            }
        } else {
            aggInstance.reset();
        }
        return aggInstance;
    }

    private static DataValueDescriptor[] getInputColumns(ExecRow row, int[] colIDs) {
        if (colIDs == null) return new DataValueDescriptor[0];
        DataValueDescriptor[] newCols = new DataValueDescriptor[colIDs.length];
        DataValueDescriptor[] cols = row.getRowArray();
        int i = 0;
        for (int colID : colIDs) {
            newCols[i++] = cols[colID -1]; // colIDs are 1-based; convert to 0-based
        }
        return newCols;
    }

    /**
     * Columns on which we are going to execute the window functions
     * @return
     */
    public int[] getInputColumnIds() {
        return inputColumnIds;
    }

    /**
     * Return the name of the window function
     * @return
     */

    public String getFunctionName() {
        return functionName;
    }

    /**
     * Full Ordering specification including asc or desc, nulls first or last
     * @return
     */
    public ColumnOrdering[] getOrderings() {
        return orderings;
    }

    /**
     * Specify the partitions of the winow query
     * @return
     */

    public ColumnOrdering[] getPartitions() {
        return partition;
    }

    /**
     * Some window functions need specific arguments to be executed
     * This the method to get them.
     * @return
     */

    public FormatableHashtable getFunctionSpecificArgs() {
        return functionSpecificArgs;
    }
}
