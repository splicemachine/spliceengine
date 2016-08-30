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
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLTime;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import com.splicemachine.derby.utils.marshall.dvd.TimestampV2DescriptorSerializer;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.LongColumnStatistics;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.LongFrequencyEstimate;
import com.splicemachine.stats.frequency.LongFrequentElements;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * @author Scott Fines
 *         Date: 3/27/15
 */
public class TimestampStatistics extends TimeStatistics{

    public TimestampStatistics(){
    }

    public TimestampStatistics(LongColumnStatistics baseStats){
        super(baseStats);
    }

    @Override public ColumnStatistics<DataValueDescriptor> getClone(){ return new TimestampStatistics((LongColumnStatistics)baseStats.getClone()); }
    @Override protected DataValueDescriptor wrap(long value){ return wrapLong(value); }

    @Override
    protected Distribution<DataValueDescriptor> newDistribution(ColumnStatistics baseStats){
        return new TimeDist((LongColumnStatistics)baseStats);
    }

    @Override
    public FrequentElements<DataValueDescriptor> topK(){
        return new TimestampFreqs((LongFrequentElements)super.baseStats.topK());
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static final Function<FrequencyEstimate<? extends Long>,
            FrequencyEstimate<DataValueDescriptor>> timestampTransform = new Function<FrequencyEstimate<? extends Long>,
                                                                        FrequencyEstimate<DataValueDescriptor>>(){
        @Override
        public FrequencyEstimate<DataValueDescriptor> apply(FrequencyEstimate<? extends Long> frequencyEstimate){
            return new TimestampFreq((LongFrequencyEstimate)frequencyEstimate);
        }
    };

    private static class TimestampFreq extends TimeFreq{
        public TimestampFreq(LongFrequencyEstimate lfe){ super(lfe); }
        @Override protected DataValueDescriptor wrap(long value){ return wrapLong(value); }
    }


    static class TimestampFreqs extends TimeStatistics.TimeFrequentElems{
        private Calendar calendar;

        public TimestampFreqs(LongFrequentElements lfe){
            super(lfe,timestampTransform);
        }

        @Override
        protected FrequentElements<DataValueDescriptor> getCopy(FrequentElements<Long> clone){
            return new TimestampFreqs((LongFrequentElements)clone);
        }

        @Override
        protected long unwrapLong(DataValueDescriptor dvd){
            if(calendar==null)
                calendar = new GregorianCalendar();
            return getLong(dvd,calendar);
        }
    }

    static long getLong(DataValueDescriptor dvd,Calendar calendar){
        try{
            return TimestampV2DescriptorSerializer.formatLong(dvd.getTimestamp(calendar));
        }catch(StandardException e){
            throw new RuntimeException(e); //shouldn't happen
        }
    }

    static DataValueDescriptor wrapLong(long value){
        Timestamp t = TimestampV2DescriptorSerializer.parseTimestamp(value);
        try{
            return new SQLTimestamp(t);
        }catch(StandardException e){
            throw new RuntimeException(e);
        }
    }

    private class TimeDist extends TimeDistribution{
        private Calendar calendar;
        public TimeDist(LongColumnStatistics baseColStats){ super(baseColStats); }

        @Override
        protected long unwrapTime(DataValueDescriptor dvd){
            if(calendar==null)
                calendar = new GregorianCalendar();
            return getLong(dvd,calendar);
        }
    }
}
