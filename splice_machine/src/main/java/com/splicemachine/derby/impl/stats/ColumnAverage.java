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
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.stats.*;
import com.splicemachine.stats.cardinality.*;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.UniformShortDistribution;
import com.splicemachine.stats.frequency.*;
import com.splicemachine.utils.ComparableComparator;
import com.splicemachine.utils.StringUtils;

import java.math.BigDecimal;
import java.sql.Time;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public abstract class ColumnAverage<T> implements ColumnStatistics<T>{
    protected final int columnId;
    private long sumNonNull;
    private long sumNull;
    private long sumCard;
    private long sumWidth;
    protected int mergeCount;
    protected CardinalityEstimator cardinalityEstimator;

    public ColumnAverage(int columnId) {
        this.columnId = columnId;
    }

    public ColumnStatistics<T> merge(ColumnStatistics<T> stats){
        sumNonNull +=stats.nonNullCount();
        sumNull +=stats.nullCount();
        sumCard +=stats.cardinality();
        sumWidth +=stats.avgColumnWidth()*stats.nonNullCount();
        if(stats instanceof ColumnAverage) {
            mergeExtrema(stats);
        }
        else if (stats instanceof BaseDvdStatistics) {
            mergeExtrema(((BaseDvdStatistics) stats).baseStats);
        }
        mergeCount++;

        return this;
    }

    @Override
    public long nonNullCount() {
        if(mergeCount<=0) return 0l;
        return sumNonNull/mergeCount;
    }

    @Override
    public long cardinality() {
        if(mergeCount<=0) return 0l;
        return sumCard/mergeCount;
    }

    @Override
    public float nullFraction() {
        if(mergeCount<=0) return 0l;
        return ((float)nullCount())/(nonNullCount()+nullCount());
    }

    @Override
    public long nullCount() {
        if(mergeCount<=0) return 0l;
        return sumNull/mergeCount;
    }

    @Override
    public int avgColumnWidth() {
        if(mergeCount<=0) return 0;
        if (sumNonNull == 0) return 0;
        return (int)(sumWidth/sumNonNull);
    }

    @Override
    public int columnId() {
        return columnId;
    }

    protected abstract void mergeExtrema(ColumnStatistics<T> stats);

    public long totalBytes() {
        return avgColumnWidth()*nonNullCount();
    }

    public static ColumnStatistics avgStats(int columnId,int typeFormatId, int maxLength){
        switch(typeFormatId){
            case StoredFormatIds.SQL_BOOLEAN_ID:
                return new BooleanAverage(columnId);
            case StoredFormatIds.SQL_TINYINT_ID:
            case StoredFormatIds.SQL_SMALLINT_ID:
                return new ShortAverage(columnId);
            case StoredFormatIds.SQL_INTEGER_ID:
                return new IntAverage(columnId);
            case StoredFormatIds.SQL_LONGINT_ID:
                return new LongAverage(columnId);
            case StoredFormatIds.SQL_REAL_ID:
                return new FloatAverage(columnId);
            case StoredFormatIds.SQL_DOUBLE_ID:
                return new DoubleAverage(columnId);
            case StoredFormatIds.SQL_DECIMAL_ID:
                return new DecimalAverage(columnId);
            case StoredFormatIds.SQL_CHAR_ID:
                return new CharAverage(columnId, maxLength);
            case StoredFormatIds.SQL_VARCHAR_ID:
            case StoredFormatIds.SQL_LONGVARCHAR_ID:
                return new VarcharAverage(columnId, maxLength);
            case StoredFormatIds.SQL_TIME_ID:
                return new TimeAverage(columnId);
            case StoredFormatIds.SQL_DATE_ID:
                return new DateAverage(columnId);
            case StoredFormatIds.SQL_TIMESTAMP_ID:
                return new TimestampAverage(columnId);
            default:
                throw new UnsupportedOperationException("Programmer error: Cannot collect statistics for format id "+ typeFormatId);
        }
    }

    public static ColumnStatistics fromExisting(ColumnStatistics toMerge) {
        ColumnStatistics ret = null;
        int columnId=toMerge.columnId();
        if(toMerge instanceof BooleanStats){
            ret = new BooleanAverage(columnId);
        } else if (toMerge instanceof SmallintStats){
            ret = new ShortAverage(columnId);
        } else if(toMerge instanceof IntStats){
            ret = new IntAverage(columnId);
        } else if(toMerge instanceof BigintStats){
            ret = new LongAverage(columnId);
        } else if(toMerge instanceof RealStats){
            ret = new FloatAverage(columnId);
        } else if(toMerge instanceof DoubleStats){
            ret = new DoubleAverage(columnId);
        } else if(toMerge instanceof NumericStats){
            ret = new DecimalAverage(columnId);
        } else if(toMerge instanceof VarcharStats){
            ret = new VarcharAverage(columnId,((VarcharStats)toMerge).maxLength());
        } else if(toMerge instanceof CharStats){
            ret = new CharAverage(columnId,((CharStats)toMerge).maxLength());
        } else if(toMerge instanceof DateStatistics){
            ret = new DateAverage(columnId);
        } else if(toMerge instanceof TimeStats){
            ret = new TimeAverage(columnId);
        } else if(toMerge instanceof TimestampStatistics){
            ret = new TimestampAverage(columnId);
        }else
             throw new IllegalArgumentException("Programmer error: unknown Column Statistics type: "+toMerge.getClass().getCanonicalName());
        ret.merge(toMerge);
        return ret;
    }

    private static class BooleanAverage extends ColumnAverage<Boolean>{
        long trueCount;
        long falseCount;

        public BooleanAverage(int columnId) {
            super(columnId);
        }

        @Override
        protected void mergeExtrema(ColumnStatistics<Boolean> stats) {
            FrequentElements<Boolean> topK = stats.topK();
            trueCount+=topK.equal(Boolean.TRUE).count();
            falseCount+=topK.equal(Boolean.FALSE).count();
        }

        @Override
        public FrequentElements<Boolean> topK() {
            BooleanFrequentElements freqs = adjustedFrequencies();
            return freqs;
        }

        @Override public Boolean minValue() { return Boolean.TRUE; }
        @Override public Boolean maxValue() { return Boolean.FALSE; }

        @Override
        public long minCount() {
            if(mergeCount<=0) return 0;
            return trueCount/mergeCount;
        }

        @Override
        public ColumnStatistics<Boolean> getClone() {
            return new BooleanAverage(columnId);
        }

        @Override
        public Distribution getDistribution() {
            BooleanFrequentElements bfe = adjustedFrequencies();
            BooleanColumnStatistics stats = new BooleanColumnStatistics(columnId,bfe,
                    avgColumnWidth()*nonNullCount(),nonNullCount(),nullCount());
            return new BooleanStats.BooleanDist(stats);
        }

        @Override
        public CardinalityEstimator getCardinalityEstimator() {
            throw new RuntimeException("getCardinalityEstimator not implemented for BooleanAverage");
        }

        private BooleanFrequentElements adjustedFrequencies() {
            long tc,fc;
            if(mergeCount<=0){
                tc = fc = 0l;
            }else{
                tc = trueCount/mergeCount;
                fc = falseCount/mergeCount;
            }
            return FrequencyCounters.booleanValues(tc, fc);
        }
    }

    private static class ShortAverage extends ColumnAverage<Short>{
        private short min=Short.MAX_VALUE;
        private long minCount;
        private short max= Short.MIN_VALUE;
        private final ShortFrequentElements empty;

        public ShortAverage(int columnId) {
            super(columnId);
            this.empty = ShortFrequentElements.topK(0, 0, Collections.<ShortFrequencyEstimate>emptyList());
        }

        @Override
        protected void mergeExtrema(ColumnStatistics<Short> stats) {
            short minV = stats.minValue();
            if(min>minV){
                min = minV;
                minCount = stats.minCount();
            }
            short maxV = stats.maxValue();
            if(max<maxV){
                max = maxV;
            }
        }

        @Override public long minCount() { return minCount; }
        @Override public ColumnStatistics<Short> getClone() { return new ShortAverage(columnId); }
        @Override public FrequentElements<Short> topK() { return empty; }

        @Override public Short minValue() { return min; }
        @Override public Short maxValue() { return max; }


        @Override
        public Distribution getDistribution() {
            ShortColumnStatistics scs = new CombinedShortColumnStatistics(columnId,
                            (ShortCardinalityEstimator)getCardinalityEstimator(),
                            ShortFrequentElements.topK(0,0,Collections.<ShortFrequencyEstimate>emptyList()),
                            min,
                            max,
                            totalBytes(),
                            nonNullCount()+nullCount(),
                            nullCount(),
                            minCount
                            );
            return new SmallintStats.ShortDist(nullCount(),new UniformShortDistribution(scs));
        }

        @Override
        public CardinalityEstimator getCardinalityEstimator() {
            if (cardinalityEstimator == null) {
                cardinalityEstimator = FixedCardinalityEstimate.shortEstimate(cardinality());
            }
            return cardinalityEstimator;
        }
    }

    private static class IntAverage extends ColumnAverage<Integer>{
        private int min=Integer.MAX_VALUE;
        private long minCount;
        private int max=Integer.MIN_VALUE;
        private final IntFrequentElements empty;

        public IntAverage(int columnId) {
            super(columnId);
            this.empty = IntFrequentElements.topK(0,0, Collections.<IntFrequencyEstimate>emptyList());
        }

        @Override
        protected void mergeExtrema(ColumnStatistics<Integer> stats) {
            int minV = stats.minValue();
            if(min>minV){
                min = minV;
                minCount = stats.minCount();
            }
            int maxV = stats.maxValue();
            if(max<maxV){
                max = maxV;
            }
        }

        @Override public long minCount() { return minCount; }
        @Override public ColumnStatistics<Integer> getClone() { return new IntAverage(columnId); }
        @Override public FrequentElements<Integer> topK() { return empty; }

        @Override public Integer minValue() { return min; }
        @Override public Integer maxValue() { return max; }


        @Override
        public Distribution getDistribution() {
            IntColumnStatistics scs = new IntColumnStatistics(columnId,
                    (IntCardinalityEstimator)getCardinalityEstimator(),
                    IntFrequentElements.topK(0,0,Collections.<IntFrequencyEstimate>emptyList()),
                    min,
                    max,
                    totalBytes(),
                    nonNullCount()+nullCount(),
                    nullCount(),
                    minCount
            );
            return new IntStats.IntDist(scs);
        }

        @Override
        public CardinalityEstimator getCardinalityEstimator() {
            if (cardinalityEstimator == null) {
                cardinalityEstimator = FixedCardinalityEstimate.intEstimate(cardinality());
            }
            return cardinalityEstimator;
        }

        private int safeInt(DataValueDescriptor min)  {
            try {
                return min.getInt();
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class LongAverage extends ColumnAverage<Long>{
        private long min=Long.MAX_VALUE;
        private long minCount;
        private long max=Long.MIN_VALUE;
        private final LongFrequentElements empty;

        public LongAverage(int columnId) {
            super(columnId);
            this.empty = LongFrequentElements.topK(0,0, Collections.<LongFrequencyEstimate>emptyList());
        }

        @Override
        protected void mergeExtrema(ColumnStatistics<Long> stats) {
            long minV = stats.minValue();
            if(min>minV){
                min = minV;
                minCount = stats.minCount();
            }
            long maxV = stats.maxValue();
            if(max<maxV){
                max = maxV;
            }
        }

        @Override public long minCount() { return minCount; }
        @Override public ColumnStatistics<Long> getClone() { return new LongAverage(columnId); }
        @Override public FrequentElements<Long> topK() { return empty; }

        @Override public Long minValue() { return min; }
        @Override public Long maxValue() { return max; }


        @Override
        public Distribution getDistribution() {
            LongColumnStatistics scs = new LongColumnStatistics(columnId,
                    (LongCardinalityEstimator)getCardinalityEstimator(),
                    LongFrequentElements.topK(0,0,Collections.<LongFrequencyEstimate>emptyList()),
                    min,
                    max,
                    totalBytes(),
                    nonNullCount()+nullCount(),
                    nullCount(),
                    minCount
            );
            return new BigintStats.LongDist(scs);
        }

        @Override
        public CardinalityEstimator getCardinalityEstimator() {
            if (cardinalityEstimator == null) {
                cardinalityEstimator = FixedCardinalityEstimate.longEstimate(cardinality());
            }
            return  cardinalityEstimator;
        }

        private long safeLong(DataValueDescriptor min)  {
            try {
                return min.getLong();
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class FloatAverage extends ColumnAverage<Float>{
        private float min=Float.MAX_VALUE;
        private long minCount;
        private float max=Float.MIN_VALUE;
        private final FloatFrequentElements empty;

        public FloatAverage(int columnId) {
            super(columnId);
            this.empty = FloatFrequentElements.topK(0,0, Collections.<FloatFrequencyEstimate>emptyList());
        }

        @Override
        protected void mergeExtrema(ColumnStatistics<Float> stats) {
            float minV = stats.minValue();
            if(min>minV){
                min = minV;
                minCount = stats.minCount();
            }
            float maxV = stats.maxValue();
            if(max<maxV){
                max = maxV;
            }
        }

        @Override public long minCount() { return minCount; }
        @Override public ColumnStatistics<Float> getClone() { return new FloatAverage(columnId); }
        @Override public FrequentElements<Float> topK() { return empty; }

        @Override public Float minValue() {
            return min;
        }
        @Override public Float maxValue() {
            return max;
        }


        @Override
        public Distribution getDistribution() {
            FloatColumnStatistics scs = new FloatColumnStatistics(columnId,
                    (FloatCardinalityEstimator)getCardinalityEstimator(),
                    FloatFrequentElements.topK(0,0,Collections.<FloatFrequencyEstimate>emptyList()),
                    min,
                    max,
                    totalBytes(),
                    nonNullCount()+nullCount(),
                    nullCount(),
                    minCount
            );
            return new RealStats.RealDist(scs);
        }

        @Override
        public CardinalityEstimator getCardinalityEstimator() {
            if (cardinalityEstimator == null) {
                cardinalityEstimator = FixedCardinalityEstimate.floatEstimate(cardinality());
            }
            return cardinalityEstimator;
        }
    }

    private static class DoubleAverage extends ColumnAverage<Double>{
        private double min=Double.MAX_VALUE;
        private long minCount;
        private double max=Double.MIN_VALUE;
        private final DoubleFrequentElements empty;

        public DoubleAverage(int columnId) {
            super(columnId);
            this.empty = DoubleFrequentElements.topK(0,0, Collections.<DoubleFrequencyEstimate>emptyList());
        }

        @Override
        protected void mergeExtrema(ColumnStatistics<Double> stats) {
            double minV = stats.minValue();
            if(min>minV){
                min = minV;
                minCount = stats.minCount();
            }
            double maxV = stats.maxValue();
            if(max<maxV){
                max = maxV;
            }
        }

        @Override public long minCount() { return minCount; }
        @Override public ColumnStatistics<Double> getClone() { return new DoubleAverage(columnId); }
        @Override public FrequentElements<Double> topK() { return empty; }

        @Override public Double minValue() {
            return min;
        }
        @Override public Double maxValue() {
           return max;
        }


        @Override
        public Distribution getDistribution() {
            DoubleColumnStatistics scs = new DoubleColumnStatistics(columnId,
                    (DoubleCardinalityEstimator)getCardinalityEstimator(),
                    DoubleFrequentElements.topK(0,0,Collections.<DoubleFrequencyEstimate>emptyList()),
                    min,
                    max,
                    totalBytes(),
                    nonNullCount()+nullCount(),
                    nullCount(),
                    minCount
            );
            return new DoubleStats.DoubleDist(scs);
        }

        @Override
        public CardinalityEstimator getCardinalityEstimator() {
            if (cardinalityEstimator == null) {
                cardinalityEstimator = FixedCardinalityEstimate.doubleEstimate(cardinality());
            }
            return cardinalityEstimator;
        }
    }

    private static class DecimalAverage extends ColumnAverage<BigDecimal>{
        private BigDecimal min;
        private long minCount;
        private BigDecimal max;
        private final ObjectFrequentElements empty;

        public DecimalAverage(int columnId) {
            super(columnId);
            this.empty = ObjectFrequentElements.topK(0,0,
                    Collections.<FrequencyEstimate<BigDecimal>>emptyList(),ComparableComparator.<BigDecimal>newComparator());
        }

        @Override
        protected void mergeExtrema(ColumnStatistics<BigDecimal> stats) {
            BigDecimal minV = stats.minValue();
            if(min==null || min.compareTo(minV)>0){
                min = minV;
                minCount = stats.minCount();
            }
            BigDecimal maxV = stats.maxValue();
            if(max==null||max.compareTo(maxV)<0){
                max = maxV;
            }
        }

        @Override public long minCount() { return minCount; }
        @Override public ColumnStatistics<BigDecimal> getClone() { return new DecimalAverage(columnId); }
        @Override public FrequentElements<BigDecimal> topK() { return empty; }

        @Override public BigDecimal minValue() { return min; }
        @Override public BigDecimal maxValue() { return max; }

        @Override
        public Distribution getDistribution() {
            ObjectFrequentElements<BigDecimal> fe = ObjectFrequentElements.topK(0, 0,
                    Collections.<FrequencyEstimate<BigDecimal>>emptyList(), ComparableComparator.<BigDecimal>newComparator());
            ColumnStatistics<BigDecimal> scs = new ComparableColumnStatistics<>(columnId,
                    (FixedCardinalityEstimate<BigDecimal>)getCardinalityEstimator(),
                    fe,
                    min,
                    max,
                    totalBytes(),
                    nonNullCount()+nullCount(),
                    nullCount(),
                    minCount,DvdStatsCollector.decimalDistributionFactory
            );
            return new NumericStats.NumericDistribution(scs);
        }

        @Override
        public CardinalityEstimator getCardinalityEstimator() {
            if (cardinalityEstimator == null) {
                cardinalityEstimator = new FixedCardinalityEstimate<BigDecimal>(cardinality());
            }
            return cardinalityEstimator;
        }
    }

    private abstract static class StringAverage extends ColumnAverage<String>{
        private String min;
        private long minCount;
        private String max;
        private final FrequentElements<String> empty;
        protected int maxLength;

        public StringAverage(int columnId,int maxLength,
                             Function<FrequencyEstimate<String>,FrequencyEstimate<DataValueDescriptor>> conversionFunction) {
            super(columnId);
            this.empty = ObjectFrequentElements.topK(0, 0,
                    Collections.<FrequencyEstimate<String>>emptyList(), ComparableComparator.<String>newComparator());
            this.maxLength = maxLength;
        }

        @Override
        protected void mergeExtrema(ColumnStatistics<String> stats) {
            String minV = stats.minValue();
            if (minV != null) {
                if (min == null || min.compareTo(minV) > 0) {
                    min = minV;
                    minCount = stats.minCount();
                }
            }
            String maxV = stats.maxValue();
            if (maxV != null) {
                if (max == null || max.compareTo(maxV) < 0) {
                    max = maxV;
                }
            }
        }

        @Override public long minCount() { return minCount; }
        @Override public FrequentElements<String> topK() { return empty; }

        @Override public String maxValue() { return max; }
        @Override public String minValue() { return min; }

        @Override
        public Distribution getDistribution() {
            ObjectFrequentElements<String> fe = ObjectFrequentElements.topK(0, 0,
                    Collections.<FrequencyEstimate<String>>emptyList(),
                    ComparableComparator.<String>newComparator());
            ColumnStatistics<String> scs = new ComparableColumnStatistics<>(columnId,
                    (FixedCardinalityEstimate<String>)getCardinalityEstimator(),
                    fe,
                    min,
                    max,
                    totalBytes(),
                    nonNullCount()+nullCount(),
                    nullCount(),
                    minCount,DvdStatsCollector.stringDistributionFactory(maxLength)
            );
            return newDistribution(scs);
        }

        @Override
        public CardinalityEstimator getCardinalityEstimator() {
            if (cardinalityEstimator == null) {
                cardinalityEstimator = new FixedCardinalityEstimate<String>(cardinality());
            }
            return cardinalityEstimator;
        }

        protected abstract Distribution<DataValueDescriptor> newDistribution(ColumnStatistics<String> scs);

        protected abstract DataValueDescriptor getDvd(String value);

        protected String safeString(DataValueDescriptor min)  {
            try {
                return min.getString();
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class CharAverage extends StringAverage{
        public CharAverage(int columnId, int maxLength) { super(columnId, maxLength,CharStats.conversionFunction); }
        @Override protected DataValueDescriptor getDvd(String value) { return new SQLChar(value); }
        @Override public ColumnStatistics<String> getClone() {return new CharAverage(columnId,maxLength);}

        @Override
        protected Distribution<DataValueDescriptor> newDistribution(ColumnStatistics<String> scs){
            return new CharStats(scs,maxLength).getDistribution();
        }
    }

    private static class VarcharAverage extends StringAverage{

        public VarcharAverage(int columnId, int maxLength) { super(columnId, maxLength,VarcharStats.conversionFunction); }
        @Override protected DataValueDescriptor getDvd(String value) { return new SQLVarchar(value); }

        @Override public ColumnStatistics<String> getClone() {
            return new VarcharAverage(columnId,maxLength);
        }

        @Override
        protected Distribution<DataValueDescriptor> newDistribution(ColumnStatistics<String> scs){
            return new VarcharStats(scs,maxLength).getDistribution();
        }

        @Override
        protected String safeString(DataValueDescriptor min){
            return StringUtils.trimTrailingSpaces(super.safeString(min));
        }
    }

    private abstract static class BaseTimeAverage extends ColumnAverage<Long>{
        private long min=Long.MAX_VALUE;
        private long minCount;
        private long max=Long.MIN_VALUE;
        private final FrequentElements<Long> empty;
        private transient Calendar calendar; //stashed for performance reasons

        public BaseTimeAverage(int columnId) {
            super(columnId);
            this.empty = LongFrequentElements.topK(0,0, Collections.<LongFrequencyEstimate>emptyList());
        }


        @Override
        protected void mergeExtrema(ColumnStatistics<Long> stats) {
            long minV=stats.minValue();
            if(min>minV){
                min=minV;
                minCount=stats.minCount();
            }
            long maxV=stats.maxValue();
            if(max<maxV) {
                max = maxV;
            }
        }

        @Override public long minCount() { return minCount; }
        @Override public FrequentElements<Long> topK() { return empty; }

        @Override public Long maxValue() { return max; }
        @Override public Long minValue() { return min; }

        @Override
        public Distribution getDistribution() {
            LongFrequentElements fe = LongFrequentElements.topK(0, 0,
                    Collections.<LongFrequencyEstimate>emptyList());
            LongColumnStatistics lcs = new LongColumnStatistics(columnId,
                    (LongCardinalityEstimator)getCardinalityEstimator(),
                    fe,
                    min,
                    max,
                    totalBytes(),
                    nonNullCount()+nullCount(),
                    nullCount(),
                    minCount);
            return newDistribution(lcs);
        }

        @Override
        public CardinalityEstimator getCardinalityEstimator() {
            if (cardinalityEstimator == null) {
                cardinalityEstimator = FixedCardinalityEstimate.longEstimate(cardinality());
            }
            return  cardinalityEstimator;
        }

        protected abstract Distribution<DataValueDescriptor> newDistribution(LongColumnStatistics lcs);

        protected abstract DataValueDescriptor getDvd(long value);

        protected abstract long safeLong(DataValueDescriptor value);

        protected abstract FrequentElements<DataValueDescriptor> emptyFreqs();

        protected Calendar getCal(){
            if(calendar==null)
                calendar = new GregorianCalendar();
            return calendar;
        }
    }

    private static class TimeAverage extends BaseTimeAverage{

        public TimeAverage(int columnId){
            super(columnId);
        }

        @Override
        protected Distribution<DataValueDescriptor> newDistribution(LongColumnStatistics lcs){
            return new TimeStats(lcs).getDistribution();
        }

        @Override
        protected DataValueDescriptor getDvd(long value){
            try{
                SQLTime sqlTime=new SQLTime();
                sqlTime.setValue(new Time(value));
                return sqlTime;
            }catch(StandardException e){
                throw new RuntimeException(e);
            }
        }

        @Override
        protected long safeLong(DataValueDescriptor value){
            try{
                return value.getTime(getCal()).getTime();
            }catch(StandardException e){
                throw new RuntimeException(e);
            }
        }

        @Override
        protected FrequentElements<DataValueDescriptor> emptyFreqs(){
            LongFrequentElements lfe=LongFrequentElements.topK(0,0,Collections.<LongFrequencyEstimate>emptyList());
            return new TimeStats.TimeFreqs(lfe);
        }

        @Override
        public ColumnStatistics<Long> getClone(){
            return new TimeAverage(columnId);
        }
    }

    private static class DateAverage extends BaseTimeAverage{

        public DateAverage(int columnId){
            super(columnId);
        }

        @Override
        protected Distribution<DataValueDescriptor> newDistribution(LongColumnStatistics lcs){
            return new DateStatistics(lcs).getDistribution();
        }

        @Override
        protected DataValueDescriptor getDvd(long value){
            try{
                SQLDate sqlTime=new SQLDate();
                sqlTime.setValue(new Time(value));
                return sqlTime;
            }catch(StandardException e){
                throw new RuntimeException(e);
            }
        }

        @Override
        protected long safeLong(DataValueDescriptor value){
            try{
                return value.getDate(getCal()).getTime();
            }catch(StandardException e){
                throw new RuntimeException(e);
            }
        }

        @Override
        protected FrequentElements<DataValueDescriptor> emptyFreqs(){
            LongFrequentElements lfe=LongFrequentElements.topK(0,0,Collections.<LongFrequencyEstimate>emptyList());
            return new DateStatistics.DateFreqs(lfe);
        }

        @Override
        public ColumnStatistics<Long> getClone(){
            return new DateAverage(columnId);
        }
    }

    private static class TimestampAverage extends BaseTimeAverage{

        public TimestampAverage(int columnId){
            super(columnId);
        }

        @Override
        protected Distribution<DataValueDescriptor> newDistribution(LongColumnStatistics lcs){
            return new TimeStats(lcs).getDistribution();
        }

        @Override
        protected DataValueDescriptor getDvd(long value){
            return TimestampStatistics.wrapLong(value);
        }

        @Override
        protected long safeLong(DataValueDescriptor value){
            return TimestampStatistics.getLong(value,getCal());
        }

        @Override
        protected FrequentElements<DataValueDescriptor> emptyFreqs(){
            LongFrequentElements lfe=LongFrequentElements.topK(0,0,Collections.<LongFrequencyEstimate>emptyList());
            return new TimestampStatistics.TimestampFreqs(lfe);
        }

        @Override
        public ColumnStatistics<Long> getClone(){
            return new TimestampAverage(columnId);
        }
    }
}
