package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLChar;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class CharStats extends StringStatistics {

    public CharStats() { }

    public CharStats(ColumnStatistics<String> stats,int strLen) {
        super(stats,strLen);
    }

    @Override protected DataValueDescriptor getDvd(String s) { return new SQLChar(s); }

    @Override
    protected Function<FrequencyEstimate<String>, FrequencyEstimate<DataValueDescriptor>> conversionFunction() {
        return conversionFunction;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ColumnStatistics<DataValueDescriptor> getClone() {
        return new CharStats((ColumnStatistics<String>)baseStats.getClone(),strLen);
    }


    /* ***************************************************************************************************************/
    /*private helper methods*/
    private static class CharFreq extends Freq{

        public CharFreq(FrequencyEstimate<String> intFrequencyEstimate) {
            super(intFrequencyEstimate);
        }

        @Override
        protected DataValueDescriptor getDvd(String value) {
            return new SQLChar(value);
        }
    }

    static final Function<FrequencyEstimate<String>,FrequencyEstimate<DataValueDescriptor>> conversionFunction = new Function<FrequencyEstimate<String>, FrequencyEstimate<DataValueDescriptor>>() {
        @Override
        public FrequencyEstimate<DataValueDescriptor> apply(FrequencyEstimate<String> stringFrequencyEstimate) {
            return new CharFreq(stringFrequencyEstimate);
        }
    };
}
