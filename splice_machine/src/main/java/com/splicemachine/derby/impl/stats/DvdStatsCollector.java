package com.splicemachine.derby.impl.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.collector.ColumnStatsCollector;
import com.splicemachine.stats.collector.ColumnStatsCollectors;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.math.BigDecimal;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public abstract class DvdStatsCollector implements ColumnStatsCollector<DataValueDescriptor>{
    protected ColumnStatsCollector collector;

    protected DvdStatsCollector(ColumnStatsCollector collector) {
        this.collector = collector;
    }

    @Override public void updateNull(long l) { collector.updateNull(l);  }
    @Override public void updateSize(int size) { collector.updateSize(size); }
    @Override public void updateNull() { updateNull(1l); }

    @Override
    public void update(DataValueDescriptor dataValueDescriptor) {
        update(dataValueDescriptor, 1l);
    }

    @Override
    public ColumnStatistics<DataValueDescriptor> build() {
        return newStats(collector.build());
    }

    @Override
    public void update(DataValueDescriptor dataValueDescriptor, long count) {
        if(dataValueDescriptor==null|| dataValueDescriptor.isNull())
            updateNull(count);
        try {
            doUpdate(dataValueDescriptor, count);
        }catch(StandardException se){
            throw new RuntimeException(se); //should never happen
        }
    }

    /**
     * @param dataValueDescriptor guaranteed to not be null <em>and </em>{@code isNull() ==false}
     * @param count the number of times the dvd occurred
     * @throws StandardException shouldn't happen, but you never know.
     */
    protected abstract void doUpdate(DataValueDescriptor dataValueDescriptor, long count) throws StandardException;

    protected abstract ColumnStatistics<DataValueDescriptor> newStats(ColumnStatistics build);

    public static ColumnStatsCollector<DataValueDescriptor> newCollector(int typeFormatId,
                                                                         int topKSize,
                                                                         int cardPrecision){
        switch(typeFormatId){
            case StoredFormatIds.SQL_BOOLEAN_ID:
                return new BooleanDvdStatsCollector(ColumnStatsCollectors.booleanCollector());
            case StoredFormatIds.SQL_TINYINT_ID:
                return new TinyintStatsCollector(ColumnStatsCollectors.byteCollector(topKSize));
            case StoredFormatIds.SQL_SMALLINT_ID:
                /*
                 * We need to make sure that the downcasting doesn't result in garbage configurations. If
                 * it does, we reset to the defaults. This should never happen, but it's put in place
                 * as a safeguard against programmer and administrator error.
                 */
                short cP = (short)cardPrecision;
                if(cP<0)
                    cP = (short)StatsConstants.DEFAULT_CARDINALITY_PRECISION;
                short topKS = (short)topKSize;
                if(topKS<0)
                    topKS =(short)StatsConstants.DEFAULT_TOPK_PRECISION;
                return new SmallintStatsCollector(ColumnStatsCollectors.shortCollector(cP,topKS));
            case StoredFormatIds.SQL_INTEGER_ID:
                return new IntDvdStatsCollector(ColumnStatsCollectors.intCollector(cardPrecision,topKSize));
            case StoredFormatIds.SQL_LONGINT_ID:
                return new BigintStatsCollector(ColumnStatsCollectors.longCollector(cardPrecision,topKSize));
            case StoredFormatIds.SQL_REAL_ID:
                return new RealStatsCollector(ColumnStatsCollectors.floatCollector(cardPrecision, topKSize));
            case StoredFormatIds.SQL_DOUBLE_ID:
                return new DoubleStatsCollector(ColumnStatsCollectors.doubleCollector(cardPrecision, topKSize));
            case StoredFormatIds.SQL_DECIMAL_ID:
                return new NumericStatsCollector(ColumnStatsCollectors.<BigDecimal>collector(cardPrecision, topKSize));
            case StoredFormatIds.SQL_CHAR_ID:
                return StringStatsCollector.charCollector(ColumnStatsCollectors.<String>collector(cardPrecision, topKSize));
            case StoredFormatIds.SQL_VARCHAR_ID:
            case StoredFormatIds.SQL_LONGVARCHAR_ID:
                return StringStatsCollector.varcharCollector(ColumnStatsCollectors.<String>collector(cardPrecision, topKSize));
            default:
                throw new UnsupportedOperationException("Programmer error: Cannot collect statistics for format id "+ typeFormatId);
        }
    }
}
