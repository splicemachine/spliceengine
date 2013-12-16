package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.UserDataValue;
import org.apache.derby.impl.sql.execute.AggregatorInfo;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

/**
 * GenericAggregator wrapper. This is a near identical copy of
 * {@link org.apache.derby.impl.sql.execute.GenericAggregator}; That class
 * has too strict of access restrictions to allow its constructive use outside of its
 * package, so this class had to be created to allow better access.
 *  
 * @author Scott Fines
 */
public class SpliceGenericAggregator {
	private static Logger LOG = Logger.getLogger(SpliceGenericAggregator.class);
	private AggregatorInfo aggInfo;
	final int aggregatorColumnId;
	private final int inputColumnId;
	private final int resultColumnId;
	
	private final ClassFactory cf;
	
	private ExecAggregator cachedAggregator;

	public SpliceGenericAggregator(AggregatorInfo aggInfo, ClassFactory cf) {
		this.aggInfo = aggInfo;
		this.cf = cf;
		this.aggregatorColumnId = aggInfo.getAggregatorColNum()+1;
		this.inputColumnId = aggInfo.getInputColNum()+1;
		this.resultColumnId = aggInfo.getOutputColNum()+1;
	}

    SpliceGenericAggregator(ExecAggregator cachedAggregator,
                            int aggregatorColumnId,
                            int inputColumnId,
                            int resultColumnId){
        this.cachedAggregator = cachedAggregator;
        this.aggInfo=null;
        this.cf=null;

        this.aggregatorColumnId = aggregatorColumnId;
        this.inputColumnId = inputColumnId;
        this.resultColumnId = resultColumnId;
    }
	
	public AggregatorInfo getAggregatorInfo(){
		return this.aggInfo;
	}

    void setAggInfo(AggregatorInfo aggInfo) {
        this.aggInfo = aggInfo;
    }

    public void accumulate(ExecRow nextRow,ExecRow accumulatorRow) throws StandardException {
		DataValueDescriptor nextCol = nextRow.getColumn(inputColumnId);
		DataValueDescriptor aggCol = accumulatorRow.getColumn(aggregatorColumnId);
		accumulate(nextCol,aggCol);
	}

	public void merge(ExecRow inputRow, ExecRow mergeRow) throws StandardException {
		DataValueDescriptor mergeCol = mergeRow.getColumn(aggregatorColumnId);
		DataValueDescriptor inputCol = inputRow.getColumn(aggregatorColumnId);
		merge(inputCol,mergeCol);
	}
	
	public boolean isDistinct(){
		return aggInfo.isDistinct();
	}
	
	public DataValueDescriptor getInputColumnValue(ExecRow row) throws StandardException{
		return row.getColumn(inputColumnId); 
	}

    public DataValueDescriptor getResultColumnValue(ExecRow row) throws StandardException{
        return row.getColumn(aggInfo.getOutputColNum()+1);
    }
	
	public boolean finish(ExecRow row) throws StandardException{
		DataValueDescriptor outputCol = row.getColumn(resultColumnId);
		DataValueDescriptor aggCol = row.getColumn(aggregatorColumnId);
		
		ExecAggregator ua = (ExecAggregator)aggCol.getObject();
		if(ua ==null) ua = getAggregatorInstance();
		
		DataValueDescriptor result = ua.getResult();
		if(result ==null) outputCol.setToNull();
		else outputCol.setValue(result);
		return ua.didEliminateNulls();
	}
	
	void merge(Storable aggregatedIn,Storable aggregatedOut) throws StandardException {
		ExecAggregator uaIn = (ExecAggregator)(((UserDataValue)aggregatedIn).getObject());
		ExecAggregator uaOut = (ExecAggregator)(((UserDataValue)aggregatedOut).getObject());
		uaOut.merge(uaIn);
	}
	
	void initialize(ExecRow row) throws StandardException{
		UserDataValue aggColumn = (UserDataValue)row.getColumn(aggregatorColumnId);
		
		ExecAggregator ua = (ExecAggregator)aggColumn.getObject();
		if(ua == null){
			ua = getAggregatorInstance();
			aggColumn.setValue(ua);
		}
	}

    boolean isInitialized(ExecRow row) throws StandardException{
        UserDataValue aggColumn = (UserDataValue)row.getColumn(aggregatorColumnId);

        ExecAggregator ua = (ExecAggregator)aggColumn.getObject();
        return ua !=null;
    }
	
	void accumulate(DataValueDescriptor inputCol,DataValueDescriptor aggCol) 
																throws StandardException{
		ExecAggregator ua = (ExecAggregator)aggCol.getObject();
		if(ua == null){
			ua = getAggregatorInstance();
		}
		ua.accumulate(inputCol,this);
	}
	
	public ExecAggregator getAggregatorInstance() throws StandardException {
		ExecAggregator aggInstance;
		if(cachedAggregator == null){
			try{
				Class aggClass = cf.loadApplicationClass(aggInfo.getAggregatorClassName());
				Object agg = aggClass.newInstance();
				aggInstance = (ExecAggregator)agg;
				cachedAggregator = aggInstance;
				aggInstance.setup(
                        cf,
                        aggInfo.getAggregateName(),
                        aggInfo.getResultDescription().getColumnInfo()[ 0 ].getType()
                );
			}catch(Exception e){
				throw StandardException.unexpectedUserException(e);
			}
		}else {
			aggInstance = cachedAggregator.newAggregator();
		}
		return aggInstance;
	}

    public void setResultColumn(ExecRow row,DataValueDescriptor result) {
        row.setColumn(resultColumnId,result);
    }

    public int getResultColumnId() {
        return resultColumnId;
    }
}
