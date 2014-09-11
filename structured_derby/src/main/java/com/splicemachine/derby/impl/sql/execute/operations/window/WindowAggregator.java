package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.util.Arrays;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.WindowFunction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.UserDataValue;
import org.apache.derby.impl.sql.execute.WindowFunctionInfo;
import org.apache.log4j.Logger;

import com.splicemachine.derby.impl.sql.execute.operations.window.function.SpliceGenericWindowFunction;

/**
 * Container and caching mechanism for window functions and their information container.
 *  
 */
public class WindowAggregator {
	private static Logger LOG = Logger.getLogger(WindowAggregator.class);
	private WindowFunctionInfo windowInfo;
	final int functionColumnId;
	private final int[] inputColumnIds;
	private final int resultColumnId;
    private final String functionName;

	private final ClassFactory cf;

	protected SpliceGenericWindowFunction cachedAggregator;

	public WindowAggregator(WindowFunctionInfo windowInfo, ClassFactory cf) {
		this.windowInfo = windowInfo;
		this.cf = cf;
        // all these are one-based
		this.functionColumnId = windowInfo.getWindowFunctionColNum();
		this.inputColumnIds = windowInfo.getInputColNums();
		this.resultColumnId = windowInfo.getOutputColNum();
        this.functionName = windowInfo.getFunctionName();
	}

    @Override
    public String toString() {
        return "WindowAggregator{" +
            "functionColumnId=" + functionColumnId +
            ", inputColumnIds=" + Arrays.toString(inputColumnIds) +
            ", resultColumnId=" + resultColumnId +
            ", functionName='" + functionName + '\'' +
            '}';
    }

    /**
     * Constructor used for testing
     * @param cachedAggregator the function to test
     * @param functionColumnId the 1-based column ID in the template row that holds the function class name
     * @param inputColumnIds the 1-based column IDs in the exec row to accept as function arguments
     * @param resultColumnId the 1-based column ID in the exec row in which to place the function result
     */
    public WindowAggregator(WindowFunction cachedAggregator,
                            int functionColumnId,
                            int[] inputColumnIds,
                            int resultColumnId){
        this.cachedAggregator = (SpliceGenericWindowFunction) cachedAggregator;
        this.windowInfo =null;
        this.cf=null;

        this.functionColumnId = functionColumnId;
        this.inputColumnIds = inputColumnIds;
        this.resultColumnId = resultColumnId;
        this.functionName = cachedAggregator.toString();
    }

    public void accumulate(ExecRow nextRow,ExecRow accumulatorRow) throws StandardException {
		DataValueDescriptor aggCol = accumulatorRow.getColumn(functionColumnId);
		accumulate(getInputColumns(nextRow, inputColumnIds),aggCol);
	}

    private static DataValueDescriptor[] getInputColumns(ExecRow row, int[] colIDs) {
        if (colIDs == null) return new DataValueDescriptor[0];
        DataValueDescriptor[] newCols = new DataValueDescriptor[colIDs.length];
        DataValueDescriptor[] cols = row.getRowArray();
        int i = 0;
        for (int colID : colIDs) {
            newCols[i++] = cols[colID -1]; // colIDs are 1-based
        }
        return newCols;
    }

	public void finish(ExecRow row) throws StandardException{
		DataValueDescriptor outputCol = row.getColumn(resultColumnId);
		DataValueDescriptor aggCol = row.getColumn(functionColumnId);

        WindowFunction ua = (WindowFunction)aggCol.getObject();
		if(ua ==null) ua = findOrCreateNewWindowFunction();
		
		DataValueDescriptor result = ua.getResult();
		if(result ==null) outputCol.setToNull();
		else outputCol.setValue(result);
	}
	
    public boolean initialize(ExecRow row) throws StandardException {
        UserDataValue aggColumn = (UserDataValue) row.getColumn(functionColumnId);

        WindowFunction ua = (WindowFunction) aggColumn.getObject();
        if (ua == null) {
            ua = findOrCreateNewWindowFunction();
            aggColumn.setValue(ua);
            return true;
        }
        return false;
    }

	public void accumulate(DataValueDescriptor[] inputCols,DataValueDescriptor aggCol)
																throws StandardException{
        WindowFunction ua = (WindowFunction)aggCol.getObject();
		if(ua == null){
			ua = findOrCreateNewWindowFunction();
		}
		ua.accumulate(inputCols);
	}

	public void initializeAndAccumulateIfNeeded(DataValueDescriptor[] inputCols,DataValueDescriptor aggCol) throws StandardException{
        WindowFunction ua = (WindowFunction)aggCol.getObject();
		if(ua == null){
			ua = findOrCreateNewWindowFunction();
			ua.accumulate(inputCols);
			aggCol.setValue(ua);
		}
	}

	
	public SpliceGenericWindowFunction findOrCreateNewWindowFunction() throws StandardException {
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
			} catch(Exception e){
				throw StandardException.unexpectedUserException(e);
			}
		} else {
            aggInstance.reset();
		}
		return aggInstance;
	}

    public int getResultColumnId() {
        return resultColumnId;
    }

    public int getFunctionColumnId() {
        return functionColumnId;
    }
}
