package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLTime;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.LongColumnStatistics;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.frequency.LongFrequencyEstimate;
import com.splicemachine.stats.frequency.LongFrequentElements;

import java.sql.Time;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * @author Scott Fines
 *         Time: 3/27/15
 */
public class TimeStats extends TimeStatistics{
    public TimeStats(){
    }

    public TimeStats(LongColumnStatistics baseStats){
        super(baseStats);
    }

    @Override public ColumnStatistics<DataValueDescriptor> getClone(){ return new TimeStats(baseStats); }
    @Override protected DataValueDescriptor wrap(long value){ return wrapLong(value); }

    @Override
    protected Distribution<DataValueDescriptor> newDistribution(ColumnStatistics baseStats){
        return new TimeDist((LongColumnStatistics)baseStats);
    }

    @Override
    public FrequentElements<DataValueDescriptor> topK(){
        return new TimeFreqs((LongFrequentElements)super.baseStats.topK());
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static final Function<FrequencyEstimate<? extends Long>,
            FrequencyEstimate<DataValueDescriptor>> timestampTransform = new Function<FrequencyEstimate<? extends Long>,
            FrequencyEstimate<DataValueDescriptor>>(){
        @Override
        public FrequencyEstimate<DataValueDescriptor> apply(FrequencyEstimate<? extends Long> frequencyEstimate){
            return new TimeFreqElem((LongFrequencyEstimate)frequencyEstimate);
        }
    };

    private static class TimeFreqElem extends TimeFreq{
        public TimeFreqElem(LongFrequencyEstimate lfe){ super(lfe); }
        @Override protected DataValueDescriptor wrap(long value){ return wrapLong(value); }
    }


    private class TimeFreqs extends TimeStatistics.TimeFrequentElems{
        private Calendar calendar;

        public TimeFreqs(LongFrequentElements lfe){
            super(lfe,timestampTransform);
        }

        @Override
        protected FrequentElements<DataValueDescriptor> getCopy(FrequentElements<Long> clone){
            return new TimeFreqs((LongFrequentElements)clone);
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
            return dvd.getTime(calendar).getTime();
        }catch(StandardException e){
            throw new RuntimeException(e); //shouldn't happen
        }
    }

    private static DataValueDescriptor wrapLong(long value){
        try{
            return new SQLTime(new Time(value));
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
