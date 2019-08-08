/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.db.client.am.Types;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.Storable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.ExecAggregator;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.UserDataValue;
import com.splicemachine.db.impl.sql.execute.*;

import java.io.Serializable;

import static com.splicemachine.db.iapi.services.io.StoredFormatIds.SQL_LONGINT_ID;
import static com.splicemachine.db.iapi.types.DataTypeDescriptor.getBuiltInDataTypeDescriptor;

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

    private ExecAggregator upgradeExecAggregator(ExecAggregator ua,
                                                 DataValueDescriptor aggCol,
                                                 StandardException e) throws StandardException {
	    // Only upgrade aggregator for overflow errors.
	    if (e != null && e.getSQLState() != SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE)
	        throw e;

        if (ua instanceof LongBufferedSumAggregator) {
            LongBufferedSumAggregator lbsa = (LongBufferedSumAggregator) ua;
            ua = lbsa.upgrade();
        }
        else if (ua instanceof AvgAggregator) {
            AvgAggregator aa = (AvgAggregator) ua;
            SumAggregator sa = null;
            if (aa.usesLongBufferedSumAggregator()) {
                aa.upgradeSumAggregator();
            }
            else if (e != null)
                throw e;
        }
        else if (e != null)
            throw e;

        // Put the new aggregator into the aggregate column,
        // as we might not be done with the aggregation yet.
        aggCol.setValue(ua);
        return ua;
    }


	public boolean finish(ExecRow row) throws StandardException{
		DataValueDescriptor outputCol = row.getColumn(resultColumnId);
		DataValueDescriptor aggCol = row.getColumn(aggregatorColumnId);
		
		ExecAggregator ua = (ExecAggregator)aggCol.getObject();
		if(ua ==null|| (ua.isUserDefinedAggregator() && ((UserDefinedAggregator) ua).getAggregator() == null))
			ua = getAggregatorInstance(row.getColumn(this.inputColumnId).getTypeFormatId());
		
		DataValueDescriptor result = null;
        try {
		    result = ua.getResult();
        }
        catch (StandardException e) {
            ua = upgradeExecAggregator(ua, aggCol, e);
            result = ua.getResult();
        }
        if (result == null)
			outputCol.setToNull();
        else {
			// Cast the result to the compile-time determined data type,
			// including precision and scale, only if there is a data type
			// mismatch, so we can avoid unnecessary overflow errors.
			// Otherwise, just use the exact result DVD directly.
            if (outputCol.getTypeFormatId() != result.getTypeFormatId())
                outputCol.setValue(result);
            else
                row.setColumn(resultColumnId, result.cloneValue(false));
        }
        return ua.didEliminateNulls();
    }
	
	public void merge(Storable aggregatedIn,Storable aggregatedOut) throws StandardException {
		ExecAggregator uaIn = (ExecAggregator)(((UserDataValue)aggregatedIn).getObject());
		ExecAggregator uaOut = (ExecAggregator)(((UserDataValue)aggregatedOut).getObject());

        // If one or the other of the two aggregate results was upgraded, we need to
        // upgrade the other now to be compatible.
        if (uaIn.getClass() != uaOut.getClass() ||
            (uaIn instanceof AvgAggregator &&
             ((AvgAggregator)uaIn).getAggregatorClass() !=
             ((AvgAggregator)uaOut).getAggregatorClass())) {
            uaIn = upgradeExecAggregator(uaIn, (DataValueDescriptor)aggregatedIn, null);
            uaOut = upgradeExecAggregator(uaOut, (DataValueDescriptor)aggregatedOut, null);
        }
        try {
		    uaOut.merge(uaIn);
        }
        catch (StandardException e) {
            uaIn = upgradeExecAggregator(uaIn, (DataValueDescriptor)aggregatedIn, e);
            uaOut = upgradeExecAggregator(uaOut, (DataValueDescriptor)aggregatedOut, e);
            uaOut.merge(uaIn);
        }

	}

    public boolean initialize(ExecRow row) throws StandardException {
        UserDataValue aggColumn = (UserDataValue) row.getColumn(aggregatorColumnId);

        ExecAggregator ua = (ExecAggregator) aggColumn.getObject();
        if (ua == null || (ua.isUserDefinedAggregator() && ((UserDefinedAggregator) ua).getAggregator() == null)) {
            ua = getAggregatorInstance(row.getColumn(this.inputColumnId).getTypeFormatId());
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
																throws StandardException {
		ExecAggregator ua = (ExecAggregator)aggCol.getObject();
		if(ua == null || (ua.isUserDefinedAggregator() && ((UserDefinedAggregator) ua).getAggregator() == null)){
			ua = getAggregatorInstance(inputCol.getTypeFormatId());
			aggCol.setValue(ua);
		}

        try {
            ua.accumulate(inputCol,this);
        }
        catch (StandardException e) {
            ua = upgradeExecAggregator(ua, aggCol, e);
            ua.accumulate(inputCol,this);
        }
    }

	public void initializeAndAccumulateIfNeeded(DataValueDescriptor inputCol,DataValueDescriptor aggCol) throws StandardException{
		ExecAggregator ua = (ExecAggregator)aggCol.getObject();
		if(ua == null || (ua.isUserDefinedAggregator() && ((UserDefinedAggregator) ua).getAggregator() == null)){
			ua = getAggregatorInstance(inputCol.getTypeFormatId());
			aggCol.setValue(ua);
            try {
                ua.accumulate(inputCol,this);
            }
            catch (StandardException e) {
                ua = upgradeExecAggregator(ua, aggCol, e);
                ua.accumulate(inputCol,this);
            }
		}
	}

	
	public ExecAggregator getAggregatorInstance(int typeFormatId) throws StandardException {
		ExecAggregator aggInstance;
		if(cachedAggregator == null){
			try{
				Class aggClass = cf.loadApplicationClass(aggInfo.getAggregatorClassName());
				Object agg = aggClass.newInstance();
				aggInstance = (ExecAggregator)agg;
				DataTypeDescriptor dtd = aggInfo.getResultDescription().getColumnInfo()[ 0 ].getType();

                                // BIGINT aggregation writes the final result to a data type of DEC(31,1), but it is
                                // faster to aggregate longs until we overflow, so do not use the result data type
                                // to determine the data type of the running SUM for this case.
                                // Do not use this optimization for AVG to make results on control
                                // consistent with native spark execution, since decimal division
                                // uses ROUND_HALF_UP while dividing a long rounds down.
                                if (typeFormatId == SQL_LONGINT_ID &&
                                    !aggInfo.getAggregateName().equals("AVG"))
                                    dtd = getBuiltInDataTypeDescriptor (Types.BIGINT, true);
                                aggInstance= aggInstance.setup(
                                        cf,
                                        aggInfo.getAggregateName(),
                                        dtd
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

	public int getInputColumnId() { return inputColumnId; }
}
