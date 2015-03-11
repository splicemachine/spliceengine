package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.frequency.FrequencyEstimate;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class VarcharStats extends StringStatistics {
    public VarcharStats() { }

    public VarcharStats(ColumnStatistics<String> stats,int strLen) {
        super(stats,strLen);
    }

    @Override protected DataValueDescriptor getDvd(String s) { return new SQLVarchar(s); }

    @Override
    protected Function<FrequencyEstimate<String>, FrequencyEstimate<DataValueDescriptor>> conversionFunction() {
        return conversionFunction;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ColumnStatistics<DataValueDescriptor> getClone() {
        return new VarcharStats((ColumnStatistics<String>)baseStats.getClone(),strLen);
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private static class CharFreq extends Freq{
        public CharFreq(FrequencyEstimate<String> intFrequencyEstimate) { super(intFrequencyEstimate); }
        @Override protected DataValueDescriptor getDvd(String value) { return new SQLVarchar(value); }
    }

    static final Function<FrequencyEstimate<String>,FrequencyEstimate<DataValueDescriptor>> conversionFunction
                                    = new Function<FrequencyEstimate<String>, FrequencyEstimate<DataValueDescriptor>>() {
        @Override
        public FrequencyEstimate<DataValueDescriptor> apply(FrequencyEstimate<String> stringFrequencyEstimate) {
            return new CharFreq(stringFrequencyEstimate);
        }
    };
}
