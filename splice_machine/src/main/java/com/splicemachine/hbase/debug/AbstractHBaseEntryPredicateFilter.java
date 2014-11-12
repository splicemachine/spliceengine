package com.splicemachine.hbase.debug;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.io.Writable;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;

public class AbstractHBaseEntryPredicateFilter<Data> extends FilterBase implements Writable {
	private static final SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();
    private EntryPredicateFilter epf;
    private EntryAccumulator accumulator;
    private EntryDecoder decoder;

    private boolean filterRow = false;
    public AbstractHBaseEntryPredicateFilter(EntryPredicateFilter epf) {
        this.epf = epf;
        this.accumulator = epf.newAccumulator();
        this.decoder = new EntryDecoder();
    }

    @Override
    public void reset() {
        this.accumulator.reset();
        this.filterRow = false;
    }

    @Override
    public boolean filterRow() {
        return filterRow;
    }

    public ReturnCode internalFilter(Data ignored) {
        if(!dataLib.singleMatchingColumn(ignored,SpliceConstants.DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES))
            return ReturnCode.INCLUDE;

        try {
            if(dataLib.getDataValuelength(ignored)==0){
                //skip records with no data
                filterRow=true;
                return ReturnCode.NEXT_COL;
            }

            decoder.set(dataLib.getDataValue(ignored));
            if(epf.match(decoder,accumulator)){
                return ReturnCode.INCLUDE;
            }else{
                filterRow = true;
                return ReturnCode.NEXT_COL;
            }
        } catch (IOException e) {
            e.printStackTrace();
            filterRow=true;
            return ReturnCode.NEXT_COL;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    }
}
