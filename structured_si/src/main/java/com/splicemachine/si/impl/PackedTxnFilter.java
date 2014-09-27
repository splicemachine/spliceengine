package com.splicemachine.si.impl;

import com.splicemachine.hbase.KeyValueUtils;
import com.splicemachine.si.api.SIFilter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.log4j.Logger;

import java.io.IOException;

public class PackedTxnFilter implements TxnFilter,SIFilter {
    static final Logger LOG = Logger.getLogger(PackedTxnFilter.class);
    protected final TxnFilter simpleFilter;
    public final RowAccumulator accumulator;
    private KeyValue lastValidKeyValue;
    protected boolean excludeRow = false;

    public PackedTxnFilter(TxnFilter simpleFilter,
													 RowAccumulator accumulator) {
        this.simpleFilter = simpleFilter;
        this.accumulator = accumulator;
    }

		public RowAccumulator getAccumulator(){
				return accumulator;
		}

    @Override
    public Filter.ReturnCode filterKeyValue(org.apache.hadoop.hbase.KeyValue dataKeyValue) throws IOException {
//        simpleFilter.setKeyValue(dataKeyValue);
        final Filter.ReturnCode returnCode = simpleFilter.filterKeyValue(dataKeyValue);
        switch (simpleFilter.getType(dataKeyValue)) {
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

		public Filter.ReturnCode doAccumulate(KeyValue dataKeyValue) throws IOException {
				if (!accumulator.isFinished() && !excludeRow && accumulator.isOfInterest(dataKeyValue)) {
						if (!accumulator.accumulate(dataKeyValue)) {
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
		public KeyValueType getType(KeyValue keyValue) throws IOException {
				return simpleFilter.getType(keyValue);
		}

		@Override
		public KeyValue produceAccumulatedKeyValue() {
				if (accumulator.isCountStar())
						return lastValidKeyValue;
				if (lastValidKeyValue == null)
						return null;
				final byte[] resultData = accumulator.result();
				if (resultData != null) {
						return KeyValueUtils.newKeyValue(lastValidKeyValue, resultData);
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
				accumulator.reset();
        lastValidKeyValue = null;
        excludeRow = false;
    }

}
