package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.util.Arrays;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.WindowFunction;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.UserDataValue;
import org.apache.derby.impl.sql.execute.WindowFunctionInfo;
import org.apache.log4j.Logger;

import com.splicemachine.derby.impl.sql.execute.operations.window.function.SpliceGenericWindowFunction;

/**
 * Container and caching mechanism for window functions and their information container.
 *  
 */
public class WindowAggregatorImpl implements WindowAggregator {
	private static Logger LOG = Logger.getLogger(WindowAggregatorImpl.class);
	private final WindowFunctionInfo windowInfo;
	final int functionColumnId;
	private final int[] inputColumnIds;
	private final int resultColumnId;
    private final String functionName;

	private final ClassFactory cf;

	protected SpliceGenericWindowFunction cachedAggregator;
    private int[] partitionColumns;
    private int[] sortColumns;
    private int[] keyColumns;
    private boolean[] keyOrders;
    private FrameDefinition frameDefinition;

    public WindowAggregatorImpl(WindowFunctionInfo windowInfo, ClassFactory cf) {
		this.windowInfo = windowInfo;
		this.cf = cf;
        // all these are one-based
		this.functionColumnId = windowInfo.getWindowFunctionColNum();
		this.inputColumnIds = windowInfo.getInputColNums();
		this.resultColumnId = windowInfo.getOutputColNum();
        this.functionName = windowInfo.getFunctionName();

        createDefinition(windowInfo);
	}

    @Override
    public String toString() {
        return "WindowAggregator{" +
            "functionName='" + functionName + '\'' +
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
    public WindowAggregatorImpl(WindowFunction cachedAggregator,
                                int functionColumnId,
                                int[] inputColumnIds,
                                int resultColumnId,
                                FrameDefinition frameDefinition){
        this.cachedAggregator = (SpliceGenericWindowFunction) cachedAggregator;
        this.windowInfo =null;
        this.cf=null;

        this.functionColumnId = functionColumnId;
        this.inputColumnIds = inputColumnIds;
        this.resultColumnId = resultColumnId;
        this.functionName = cachedAggregator.toString();
        this.frameDefinition = frameDefinition;
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
    public int[] getPartitionColumns() {
        return partitionColumns;
    }

    @Override
    public int[] getKeyColumns() {
        return keyColumns;
    }

    @Override
    public int[] getSortColumns() {
        return sortColumns;
    }

    @Override
    public boolean[] getKeyOrders() {
        return keyOrders;
    }

    @Override
    public FrameDefinition getFrameDefinition() {
        return frameDefinition;
    }

    @Override
    public String getName() {
        return functionName;
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
                Class aggClass = cf.loadApplicationClass(windowInfo.getWindowFunctionClassName());
                WindowFunction function = (WindowFunction) aggClass.newInstance();
                function.setup(cf,
                        windowInfo.getFunctionName(),
                        windowInfo.getResultDescription().getColumnInfo()[ 0 ].getType()
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

    private void createDefinition(WindowFunctionInfo windowInfo) {
        // create the frame from passed in frame info
        frameDefinition = FrameDefinition.create(windowInfo.getFrameInfo());

        ColumnOrdering[] partition = windowInfo.getPartitionInfo();
        partitionColumns = new int[partition.length];
        int index=0;
        for(ColumnOrdering partCol : partition){
            // coming from Derby, these are 1-based column positions
            // convert to 0-based
            partitionColumns[index++] = partCol.getColumnId() -1;
        }

        ColumnOrdering[] orderings = windowInfo.getOrderByInfo();
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
        index = 0;
        for (ColumnOrdering key : keys) {
            // coming from Derby, these are 1-based column positions
            // convert to 0-based
            keyColumns[index] = key.getColumnId() -1;
            keyOrders[index++] = key.getIsAscending();
        }
    }
}
