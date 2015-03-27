package com.splicemachine.derby.impl.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.utils.marshall.dvd.TimestampV2DescriptorSerializer;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.LongColumnStatistics;
import com.splicemachine.stats.collector.LongColumnStatsCollector;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * @author Scott Fines
 *         Date: 3/27/15
 */
public abstract class TimeCollector extends DvdStatsCollector{
    protected LongColumnStatsCollector baseCollector;

    protected TimeCollector(LongColumnStatsCollector collector){
        super(collector);
        this.baseCollector = baseCollector;
    }

    @Override
    protected void doUpdate(DataValueDescriptor dataValueDescriptor,
                            long count) throws StandardException{
        baseCollector.update(getLong(dataValueDescriptor),count);

    }

    protected abstract long getLong(DataValueDescriptor dataValueDescriptor) throws StandardException;

    public static TimeCollector timestamp(LongColumnStatsCollector baseCollector){
        return new TimestampCollector(baseCollector);
    }

    public static TimeCollector date(LongColumnStatsCollector baseCollector){
        return new DateCollector(baseCollector);
    }

    public static TimeCollector time(LongColumnStatsCollector baseCollector){
        return new TimeStatsCollector(baseCollector);
    }

    public static class TimestampCollector extends TimeCollector{
        private Calendar calendar;

        protected TimestampCollector(LongColumnStatsCollector collector){
            super(collector);
        }

        @Override
        protected long getLong(DataValueDescriptor dataValueDescriptor) throws StandardException{
            if(calendar==null)
                calendar = new GregorianCalendar(); //TODO -sf- is this right?
            Timestamp timestamp=dataValueDescriptor.getTimestamp(calendar);
            return TimestampV2DescriptorSerializer.formatLong(timestamp);
        }

        @Override
        protected ColumnStatistics<DataValueDescriptor> newStats(ColumnStatistics build){
            return new TimestampStatistics((LongColumnStatistics)build);
        }
    }

    public static class DateCollector extends TimeCollector{
        private Calendar calendar;

        protected DateCollector(LongColumnStatsCollector collector){
            super(collector);
        }

        @Override
        protected long getLong(DataValueDescriptor dataValueDescriptor) throws StandardException{
            if(calendar==null)
                calendar = new GregorianCalendar(); //TODO -sf- is this right?
            return dataValueDescriptor.getDate(calendar).getTime();
        }

        @Override
        protected ColumnStatistics<DataValueDescriptor> newStats(ColumnStatistics build){
            return new DateStatistics((LongColumnStatistics)build);
        }
    }

    public static class TimeStatsCollector extends TimeCollector{
        private Calendar calendar;

        protected TimeStatsCollector(LongColumnStatsCollector collector){
            super(collector);
        }

        @Override
        protected long getLong(DataValueDescriptor dataValueDescriptor) throws StandardException{
            if(calendar==null)
                calendar = new GregorianCalendar(); //TODO -sf- is this right?
            return dataValueDescriptor.getTime(calendar).getTime();
        }

        @Override
        protected ColumnStatistics<DataValueDescriptor> newStats(ColumnStatistics build){
            return new TimeStats((LongColumnStatistics)build);
        }
    }

}
