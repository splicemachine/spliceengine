package com.splicemachine.si.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;

import com.splicemachine.hbase.CellUtils;

public class FilterStatePacked<Result, Put extends OperationWithAttributes, Delete, Get extends OperationWithAttributes,
        Scan, Lock, OperationStatus, Mutation extends OperationWithAttributes, IHTable> implements IFilterState {
    static final Logger LOG = Logger.getLogger(FilterStatePacked.class);
    protected final FilterState<Result, Put, Delete, Get, Scan, Lock, OperationStatus,
						Mutation, IHTable> simpleFilter;
    protected final RowAccumulator accumulator;
    private Cell lastValidKeyValue;
    protected boolean excludeRow = false;

    public FilterStatePacked(FilterState<Result, Put, Delete, Get, Scan, Lock,
                            OperationStatus, Mutation, IHTable> simpleFilter,
                             RowAccumulator accumulator) {
        this.simpleFilter = simpleFilter;
        this.accumulator = accumulator;
    }

		public RowAccumulator getAccumulator(){
				return accumulator;
		}

    @Override
    public Filter.ReturnCode filterCell(Cell dataKeyValue) throws IOException {
        simpleFilter.setKeyValue(dataKeyValue);
        final Filter.ReturnCode returnCode = simpleFilter.filterByColumnType();
        switch (simpleFilter.type) {
            case COMMIT_TIMESTAMP:
                return returnCode; // These are always skip...
            case USER_DATA:
                switch (returnCode) {
                	case INCLUDE:
                	case INCLUDE_AND_NEXT_COL:
						return doAccumulate(dataKeyValue);
                    case SKIP:
                    case NEXT_COL:
                    case NEXT_ROW:
                    	return skipRow();
                    default:
                    	throw new RuntimeException("unknown return code");
                }
            case TOMBSTONE:
            case ANTI_TOMBSTONE:
            case OTHER:
                return returnCode; // These are always skip...

            default:
            	throw new RuntimeException("unknown key value type");
        }
    }

		protected Filter.ReturnCode skipRow() {
				return Filter.ReturnCode.SKIP;
		}

		protected Filter.ReturnCode doAccumulate(Cell dataKeyValue) throws IOException {
				if (!accumulator.isFinished() && !excludeRow && accumulator.isOfInterest(dataKeyValue)) {
						if (!accumulator.accumulate(simpleFilter.keyValue.keyValue())) {
								excludeRow = true;
						}
				}
				if (lastValidKeyValue == null) {
						lastValidKeyValue = dataKeyValue;
						return Filter.ReturnCode.INCLUDE;
				}
				return Filter.ReturnCode.SKIP;
		}


		@Override
		public Cell produceAccumulatedCell() {
				if (accumulator.isCountStar())
						return lastValidKeyValue;
				if (lastValidKeyValue == null)
						return null;
				final byte[] resultData = accumulator.result();
				if (resultData != null) {
						return CellUtils.newKeyValue(lastValidKeyValue, resultData);
				} else {
						return null;
				}
		}

		@Override
		public boolean getExcludeRow() {
				return excludeRow || lastValidKeyValue == null;
		}

    @Override
    public void nextRow() {
        simpleFilter.nextRow();
        lastValidKeyValue = null;
        excludeRow = false;
    }

}
