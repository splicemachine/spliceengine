/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.utils.StringUtils;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class VarcharStats extends StringStatistics {
    public VarcharStats() { }

    public VarcharStats(ColumnStatistics<String> stats,int strLen) {
        super(stats,strLen);
    }

    @Override
    public FrequentElements<DataValueDescriptor> topK(){
        return new VarcharFreqs(baseStats.topK(),conversionFunction());
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

    @Override
    protected String safeGetString(DataValueDescriptor element){
        return StringUtils.trimTrailingSpaces(super.safeGetString(element));
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private static class CharFreq extends Freq{
        public CharFreq(FrequencyEstimate<String> intFrequencyEstimate) { super(intFrequencyEstimate); }
        @Override protected DataValueDescriptor getDvd(String value) { return new SQLVarchar(value); }
    }

    private static class VarcharFreqs extends Freqs{

        public VarcharFreqs(FrequentElements<String> freqs,Function<FrequencyEstimate<String>, FrequencyEstimate<DataValueDescriptor>> conversionFunction){
            super(freqs,conversionFunction);
        }

        @Override
        public FrequentElements<DataValueDescriptor> getClone(){
            return new VarcharFreqs(frequentElements.getClone(),conversionFunction);
        }

        @Override
        protected String safeGetString(DataValueDescriptor element){
            return StringUtils.trimTrailingSpaces(super.safeGetString(element));
        }
    }

    static final Function<FrequencyEstimate<String>,FrequencyEstimate<DataValueDescriptor>> conversionFunction
                                    = new Function<FrequencyEstimate<String>, FrequencyEstimate<DataValueDescriptor>>() {
        @Override
        public FrequencyEstimate<DataValueDescriptor> apply(FrequencyEstimate<String> stringFrequencyEstimate) {
            return new CharFreq(stringFrequencyEstimate);
        }
    };
}
