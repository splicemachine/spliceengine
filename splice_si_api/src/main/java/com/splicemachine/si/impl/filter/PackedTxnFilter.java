package com.splicemachine.si.impl.filter;

import com.splicemachine.si.api.data.SReturnCodeLib;
import com.splicemachine.si.api.filter.RowAccumulator;
import com.splicemachine.si.api.filter.SIFilter;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.api.txn.KeyValueType;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.log4j.Logger;
import java.io.IOException;

public class PackedTxnFilter<Data,ReturnCode> implements TxnFilter<Data,ReturnCode>,SIFilter<Data,ReturnCode> {
    static final Logger LOG = Logger.getLogger(PackedTxnFilter.class);
    protected final TxnFilter<Data,ReturnCode> simpleFilter;
    public final RowAccumulator<Data> accumulator;
    private Data lastValidKeyValue;
    protected boolean excludeRow = false;
    protected SReturnCodeLib<ReturnCode> returnCodeLib = SIDriver.getReturnCodeLib();

    public PackedTxnFilter(TxnFilter simpleFilter, RowAccumulator accumulator) {
        this.simpleFilter = simpleFilter;
        this.accumulator = accumulator;
    }

		public RowAccumulator getAccumulator(){
				return accumulator;
		}

    @Override
    public ReturnCode filterKeyValue(Data data) throws IOException {
        final ReturnCode returnCode = simpleFilter.filterKeyValue(data);
        switch (simpleFilter.getType(data)) {
            case COMMIT_TIMESTAMP:
                return returnCode; // These are always skip...
            case USER_DATA:
                if (returnCodeLib.isInclude(returnCode) || returnCodeLib.isIncludeAndNextCol(returnCode))
				    return doAccumulate(data);
                if (returnCodeLib.isSkip(returnCode) || returnCodeLib.isNextCol(returnCode) || returnCodeLib.isNextRow(returnCode))
                    return skipRow();
                throw new RuntimeException("unknown return code");
            case TOMBSTONE:
            case ANTI_TOMBSTONE:
            case FOREIGN_KEY_COUNTER:
            case OTHER:
                return returnCode; // These are always skip...

            default:
            	throw new RuntimeException("unknown key value type");
        }
    }

		protected ReturnCode skipRow() {
				return returnCodeLib.getSkipReturnCode();
		}

		public ReturnCode doAccumulate(Data data) throws IOException {
				if (!accumulator.isFinished() && !excludeRow && accumulator.isOfInterest(data)) {
						if (!accumulator.accumulate(data)) {
								excludeRow = true;
						}
				}
				if (lastValidKeyValue == null) {
						lastValidKeyValue = data;
						return returnCodeLib.getIncludeReturnCode();
				}
				return returnCodeLib.getSkipReturnCode();
		}

		@Override
		public KeyValueType getType(Data data) throws IOException {
				return simpleFilter.getType(data);
		}

		@Override
		public Data produceAccumulatedKeyValue() {
				if (accumulator.isCountStar())
						return lastValidKeyValue;
				if (lastValidKeyValue == null)
						return null;
				final byte[] resultData = accumulator.result();
				if (resultData != null) {
						return (Data) getDataStore().dataLib.newValue(lastValidKeyValue, resultData);
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

	@Override
	public DataStore getDataStore() {
		return simpleFilter.getDataStore();
	}

}
