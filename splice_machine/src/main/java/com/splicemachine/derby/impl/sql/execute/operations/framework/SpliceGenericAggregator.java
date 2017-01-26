/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations.framework;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.Storable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.ExecAggregator;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.UserDataValue;
import com.splicemachine.db.impl.sql.execute.AggregatorInfo;
import com.splicemachine.db.impl.sql.execute.UserDefinedAggregator;
import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * GenericAggregator wrapper. This is a near identical copy of
 * {@link com.splicemachine.db.impl.sql.execute.GenericAggregator}; That class
 * has too strict of access restrictions to allow its constructive use outside of its
 * package, so this class had to be created to allow better access.
 *  
 * @author Scott Fines
 */
public class SpliceGenericAggregator implements Serializable{
	private static final long serialVersionUID = 1l;
	private AggregatorInfo aggInfo;
	final int aggregatorColumnId;
	private final int inputColumnId;
	private final int resultColumnId;
	
	private transient final ClassFactory cf;
	
	protected ExecAggregator cachedAggregator;

	public SpliceGenericAggregator(AggregatorInfo aggInfo, ClassFactory cf) {
		this.aggInfo = aggInfo;
		this.cf = cf;
		this.aggregatorColumnId = aggInfo.getAggregatorColNum()+1;
		this.inputColumnId = aggInfo.getInputColNum()+1;
		this.resultColumnId = aggInfo.getOutputColNum()+1;
	}

    public SpliceGenericAggregator(ExecAggregator cachedAggregator,
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

    public void setAggInfo(AggregatorInfo aggInfo) {
        this.aggInfo = aggInfo;
    }

    public void initializeAndAccumulateIfNeeded(ExecRow nextRow,ExecRow accumulatorRow) throws StandardException {
		DataValueDescriptor nextCol = nextRow.getColumn(inputColumnId);
		DataValueDescriptor aggCol = accumulatorRow.getColumn(aggregatorColumnId);
		initializeAndAccumulateIfNeeded(nextCol,aggCol);
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
		if(ua ==null|| (ua.isUserDefinedAggregator() && ((UserDefinedAggregator) ua).getAggregator() == null))
			ua = getAggregatorInstance();
		
		DataValueDescriptor result = ua.getResult();
		if(result ==null) outputCol.setToNull();
		else outputCol.setValue(result);
		return ua.didEliminateNulls();
	}
	
	public void merge(Storable aggregatedIn,Storable aggregatedOut) throws StandardException {
		ExecAggregator uaIn = (ExecAggregator)(((UserDataValue)aggregatedIn).getObject());
		ExecAggregator uaOut = (ExecAggregator)(((UserDataValue)aggregatedOut).getObject());
		uaOut.merge(uaIn);
	}

    public boolean initialize(ExecRow row) throws StandardException {
        UserDataValue aggColumn = (UserDataValue) row.getColumn(aggregatorColumnId);

        ExecAggregator ua = (ExecAggregator) aggColumn.getObject();
        if (ua == null || (ua.isUserDefinedAggregator() && ((UserDefinedAggregator) ua).getAggregator() == null)) {
            ua = getAggregatorInstance();
            aggColumn.setValue(ua);
            return true;
        }
        return false;
    }

    public boolean isInitialized(ExecRow row) throws StandardException{
        UserDataValue aggColumn = (UserDataValue)row.getColumn(aggregatorColumnId);

        ExecAggregator ua = (ExecAggregator)aggColumn.getObject();
		if (ua == null || (ua.isUserDefinedAggregator() && ((UserDefinedAggregator) ua).getAggregator() == null)) {
			return false;
		}

		return true;
    }
    
	public void accumulate(DataValueDescriptor inputCol,DataValueDescriptor aggCol) 
																throws StandardException{
		ExecAggregator ua = (ExecAggregator)aggCol.getObject();
		if(ua == null || (ua.isUserDefinedAggregator() && ((UserDefinedAggregator) ua).getAggregator() == null)){
			ua = getAggregatorInstance();
		}
		ua.accumulate(inputCol,this);
	}

	public void initializeAndAccumulateIfNeeded(DataValueDescriptor inputCol,DataValueDescriptor aggCol) throws StandardException{
		ExecAggregator ua = (ExecAggregator)aggCol.getObject();
		if(ua == null || (ua.isUserDefinedAggregator() && ((UserDefinedAggregator) ua).getAggregator() == null)){
			ua = getAggregatorInstance();
			ua.accumulate(inputCol,this);
			aggCol.setValue(ua);
		}
	}

	
	public ExecAggregator getAggregatorInstance() throws StandardException {
		ExecAggregator aggInstance;
		if(cachedAggregator == null){
			try{
				Class aggClass = cf.loadApplicationClass(aggInfo.getAggregatorClassName());
				Object agg = aggClass.newInstance();
				aggInstance = (ExecAggregator)agg;
				aggInstance= aggInstance.setup(
                        cf,
                        aggInfo.getAggregateName(),
                        aggInfo.getResultDescription().getColumnInfo()[ 0 ].getType()
                );
					cachedAggregator = aggInstance;
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

    public int getAggregatorColumnId() {
        return aggregatorColumnId;
    }
}
