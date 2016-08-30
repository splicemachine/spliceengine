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
import com.splicemachine.db.iapi.types.SQLDate;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.LongColumnStatistics;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.LongFrequencyEstimate;
import com.splicemachine.stats.frequency.LongFrequentElements;

import java.sql.Date;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * @author Scott Fines
 *         Date: 3/27/15
 */
public class DateStatistics extends TimeStatistics{

    public DateStatistics(){
    }

    public DateStatistics(LongColumnStatistics baseStats){
        super(baseStats);
    }

    @Override public ColumnStatistics<DataValueDescriptor> getClone(){ return new DateStatistics((LongColumnStatistics)baseStats.getClone()); }
    @Override protected DataValueDescriptor wrap(long value){ return wrapLong(value); }

    @Override
    protected Distribution<DataValueDescriptor> newDistribution(ColumnStatistics baseStats){
        return new DateDist((LongColumnStatistics)baseStats);
    }

    @Override
    public FrequentElements<DataValueDescriptor> topK(){
        return new DateFreqs((LongFrequentElements)super.baseStats.topK());
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static final Function<FrequencyEstimate<? extends Long>,
            FrequencyEstimate<DataValueDescriptor>> timestampTransform = new Function<FrequencyEstimate<? extends Long>,
            FrequencyEstimate<DataValueDescriptor>>(){
        @Override
        public FrequencyEstimate<DataValueDescriptor> apply(FrequencyEstimate<? extends Long> frequencyEstimate){
            return new DateFreq((LongFrequencyEstimate)frequencyEstimate);
        }
    };

    static class DateFreq extends TimeFreq{
        public DateFreq(LongFrequencyEstimate lfe){ super(lfe); }
        @Override protected DataValueDescriptor wrap(long value){ return wrapLong(value); }
    }


    static class DateFreqs extends TimeStatistics.TimeFrequentElems{
        private Calendar calendar;

        public DateFreqs(LongFrequentElements lfe){
            super(lfe,timestampTransform);
        }

        @Override
        protected FrequentElements<DataValueDescriptor> getCopy(FrequentElements<Long> clone){
            return new DateFreqs((LongFrequentElements)clone);
        }

        @Override
        protected long unwrapLong(DataValueDescriptor dvd){
            if(calendar==null)
                calendar = new GregorianCalendar();
            return getLong(dvd,calendar);
        }
    }

    private static long getLong(DataValueDescriptor dvd,Calendar calendar){
        try{
            return dvd.getDate(calendar).getTime();
        }catch(StandardException e){
            throw new RuntimeException(e); //shouldn't happen
        }
    }

    private static DataValueDescriptor wrapLong(long value){
        try{
            return new SQLDate(new Date(value));
        }catch(StandardException e){
            throw new RuntimeException(e);
        }
    }

    class DateDist extends TimeDistribution{
        private Calendar calendar;
        public DateDist(LongColumnStatistics baseColStats){ super(baseColStats); }

        @Override
        protected long unwrapTime(DataValueDescriptor dvd){
            if(calendar==null)
                calendar = new GregorianCalendar();
            return getLong(dvd,calendar);
        }
    }
}
