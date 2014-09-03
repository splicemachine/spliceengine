package com.splicemachine.derby.impl.sql.execute.operations.window;

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
 * GenericAggregator wrapper. This is a near identical copy of
 * {@link org.apache.derby.impl.sql.execute.GenericAggregator}; That class
 * has too strict of access restrictions to allow its constructive use outside of its
 * package, so this class had to be created to allow better access.
 *  
 */
public class WindowAggregator {
	private static Logger LOG = Logger.getLogger(WindowAggregator.class);
	private WindowFunctionInfo aggInfo;
	final int aggregatorColumnId;
	private final int[] inputColumnIds;
	private final int resultColumnId;

	private final ClassFactory cf;

	protected SpliceGenericWindowFunction cachedAggregator;

	public WindowAggregator(WindowFunctionInfo aggInfo, ClassFactory cf) {
		this.aggInfo = aggInfo;
		this.cf = cf;
        // TODO: assure all these are one-based
		this.aggregatorColumnId = aggInfo.getWindowFunctionColNum();
		this.inputColumnIds = aggInfo.getInputColNums();
		this.resultColumnId = aggInfo.getOutputColNum();
	}

    public WindowAggregator(WindowFunction cachedAggregator,
                            int aggregatorColumnId,
                            int[] inputColumnIds,
                            int resultColumnId){
        this.cachedAggregator = (SpliceGenericWindowFunction) cachedAggregator;
        this.aggInfo=null;
        this.cf=null;

        this.aggregatorColumnId = aggregatorColumnId;
        this.inputColumnIds = inputColumnIds;
        this.resultColumnId = resultColumnId;
    }
	
	public WindowFunctionInfo getAggregatorInfo(){
		return this.aggInfo;
	}

    public void setWindowFunctionInfo(WindowFunctionInfo aggInfo) {
        this.aggInfo = aggInfo;
    }

    public void initializeAndAccumulateIfNeeded(ExecRow nextRow,ExecRow accumulatorRow) throws StandardException {
		DataValueDescriptor aggCol = accumulatorRow.getColumn(aggregatorColumnId);
		initializeAndAccumulateIfNeeded(getInputColumns(accumulatorRow, inputColumnIds),aggCol);
	}

    
    public void accumulate(ExecRow nextRow,ExecRow accumulatorRow) throws StandardException {
		DataValueDescriptor aggCol = accumulatorRow.getColumn(aggregatorColumnId);
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

    public DataValueDescriptor getResultColumnValue(ExecRow row) throws StandardException{
        return row.getColumn(aggInfo.getOutputColNum()+1);
    }
	
	public void finish(ExecRow row) throws StandardException{
		DataValueDescriptor outputCol = row.getColumn(resultColumnId);
		DataValueDescriptor aggCol = row.getColumn(aggregatorColumnId);

        WindowFunction ua = (WindowFunction)aggCol.getObject();
		if(ua ==null) ua = findOrCreateNewWindowFunction();
		
		DataValueDescriptor result = ua.getResult();
		if(result ==null) outputCol.setToNull();
		else outputCol.setValue(result);
	}
	
    public boolean initialize(ExecRow row) throws StandardException {
        UserDataValue aggColumn = (UserDataValue) row.getColumn(aggregatorColumnId);

        WindowFunction ua = (WindowFunction) aggColumn.getObject();
        if (ua == null) {
            ua = findOrCreateNewWindowFunction();
            aggColumn.setValue(ua);
            return true;
        }
        return false;
    }

    public boolean isInitialized(ExecRow row) throws StandardException{
        UserDataValue aggColumn = (UserDataValue)row.getColumn(aggregatorColumnId);

        WindowFunction ua = (WindowFunction)aggColumn.getObject();
        return ua !=null;
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
				Class aggClass = cf.loadApplicationClass(aggInfo.getWindowFunctionClassName());
                WindowFunction function = (WindowFunction) aggClass.newInstance();
                function.setup(cf,
                               aggInfo.getFunctionName(),
                               aggInfo.getResultDescription().getColumnInfo()[ 0 ].getType()
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

    public void setResultColumn(ExecRow row,DataValueDescriptor result) {
        row.setColumn(resultColumnId,result);
    }

    public int getResultColumnId() {
        return resultColumnId;
    }

    public int getAggregatorColumnId() {
        return aggregatorColumnId;
    }
}
