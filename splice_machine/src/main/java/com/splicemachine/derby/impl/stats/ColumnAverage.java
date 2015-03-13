package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.stats.*;
import com.splicemachine.stats.cardinality.FixedCardinalityEstimate;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.UniformShortDistribution;
import com.splicemachine.stats.frequency.*;
import com.splicemachine.utils.ComparableComparator;

import java.math.BigDecimal;
import java.util.Collections;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public abstract class ColumnAverage implements ColumnStatistics<DataValueDescriptor>{
    protected final int columnId;
    private long sumNonNull;
    private long sumNull;
    private long sumCard;
    private long sumWidth;
    protected int mergeCount;

    public ColumnAverage(int columnId) {
        this.columnId = columnId;
    }

    public ColumnStatistics<DataValueDescriptor> merge(ColumnStatistics<DataValueDescriptor> stats){
        sumNonNull +=stats.nonNullCount();
        sumNull +=stats.nullCount();
        sumCard +=stats.cardinality();
        sumWidth +=stats.avgColumnWidth()*stats.nonNullCount();
        mergeExtrema(stats);
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
        return nullCount()/(nonNullCount()+nullCount());
    }

    @Override
    public long nullCount() {
        if(mergeCount<=0) return 0l;
        return sumNull/mergeCount;
    }

    @Override
    public int avgColumnWidth() {
        if(mergeCount<=0) return 0;
        return (int)(sumWidth/sumNonNull);
    }

    @Override
    public int columnId() {
        return columnId;
    }

    protected abstract void mergeExtrema(ColumnStatistics<DataValueDescriptor> stats);

    protected long totalBytes() {
        return avgColumnWidth()*nonNullCount();
    }

    public static ColumnStatistics<DataValueDescriptor> avgStats(int columnId,int typeFormatId, int maxLength){
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
            default:
                throw new UnsupportedOperationException("Programmer error: Cannot collect statistics for format id "+ typeFormatId);
        }
    }

    public static ColumnStatistics fromExisting(ColumnStatistics toMerge) {
        if(toMerge instanceof BooleanStats){
            return new BooleanAverage(toMerge.columnId());
        } else if (toMerge instanceof SmallintStats){
            return new ShortAverage(toMerge.columnId());
        } else if(toMerge instanceof IntStats){
            return new IntAverage(toMerge.columnId());
        } else if(toMerge instanceof BigintStats){
            return new LongAverage(toMerge.columnId());
        } else if(toMerge instanceof RealStats){
            return new FloatAverage(toMerge.columnId());
        } else if(toMerge instanceof DoubleStats){
            return new DoubleAverage(toMerge.columnId());
        } else if(toMerge instanceof NumericStats){
            return new DecimalAverage(toMerge.columnId());
        } else if(toMerge instanceof VarcharStats){
            return new VarcharAverage(toMerge.columnId(),((VarcharStats)toMerge).maxLength());
        } else if(toMerge instanceof CharStats){
            return new CharAverage(toMerge.columnId(),((CharStats)toMerge).maxLength());
        }else
             throw new IllegalArgumentException("Programmer error: unknown Column Statistics type: "+toMerge.getClass().getCanonicalName());
    }

    private static class BooleanAverage extends ColumnAverage{
        long trueCount;
        long falseCount;

        public BooleanAverage(int columnId) {
            super(columnId);
        }

        @Override
        protected void mergeExtrema(ColumnStatistics<DataValueDescriptor> stats) {
            FrequentElements<DataValueDescriptor> topK = stats.topK();
            trueCount+=topK.equal(SQLBoolean.trueTruthValue()).count();
            falseCount+=topK.equal(SQLBoolean.falseTruthValue()).count();
        }

        @Override
        public FrequentElements<DataValueDescriptor> topK() {
            BooleanFrequentElements freqs = adjustedFrequencies();
            return new BooleanStats.BooleanFreqs(freqs);
        }

        @Override public DataValueDescriptor minValue() { return SQLBoolean.trueTruthValue(); }
        @Override public DataValueDescriptor maxValue() { return SQLBoolean.falseTruthValue(); }

        @Override
        public long minCount() {
            if(mergeCount<=0) return 0;
            return trueCount/mergeCount;
        }

        @Override
        public ColumnStatistics<DataValueDescriptor> getClone() {
            return new BooleanAverage(columnId);
        }

        @Override
        public Distribution<DataValueDescriptor> getDistribution() {
            BooleanFrequentElements bfe = adjustedFrequencies();
            BooleanColumnStatistics stats = new BooleanColumnStatistics(columnId,bfe,
                    avgColumnWidth()*nonNullCount(),nonNullCount(),nullCount(),trueCount);
            return new BooleanStats.BooleanDist(stats);
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

    private static class ShortAverage extends ColumnAverage{
        private short min;
        private long minCount;
        private short max;
        private final FrequentElements<DataValueDescriptor> empty;

        public ShortAverage(int columnId) {
            super(columnId);
            ShortFrequentElements freqs = ShortFrequentElements.topK(0, 0, Collections.<ShortFrequencyEstimate>emptyList());
            this.empty = new SmallintStats.ShortFreqs(freqs);
        }

        @Override
        protected void mergeExtrema(ColumnStatistics<DataValueDescriptor> stats) {
            DataValueDescriptor minDvd = stats.minValue();
            short minV = safeShort(minDvd);
            if(min>minV){
                min = minV;
                minCount = stats.minCount();
            }
            short maxV = safeShort(stats.maxValue());
            if(max<maxV){
                max = maxV;
            }
        }

        @Override public long minCount() { return minCount; }
        @Override public ColumnStatistics<DataValueDescriptor> getClone() { return new ShortAverage(columnId); }
        @Override public FrequentElements<DataValueDescriptor> topK() { return empty; }

        @Override public DataValueDescriptor minValue() { return new SQLSmallint(min); }
        @Override public DataValueDescriptor maxValue() { return new SQLSmallint(max); }


        @Override
        public Distribution<DataValueDescriptor> getDistribution() {
            ShortColumnStatistics scs = new CombinedShortColumnStatistics(columnId,
                    FixedCardinalityEstimate.shortEstimate(cardinality()),
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

        private short safeShort(DataValueDescriptor min)  {
            try {
                return min.getShort();
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class IntAverage extends ColumnAverage{
        private int min;
        private long minCount;
        private int max;
        private final FrequentElements<DataValueDescriptor> empty;

        public IntAverage(int columnId) {
            super(columnId);
            this.empty = new IntStats.IntFreqs(IntFrequentElements.topK(0,0, Collections.<IntFrequencyEstimate>emptyList()));
        }

        @Override
        protected void mergeExtrema(ColumnStatistics<DataValueDescriptor> stats) {
            DataValueDescriptor minDvd = stats.minValue();
            int minV = safeInt(minDvd);
            if(min>minV){
                min = minV;
                minCount = stats.minCount();
            }
            int maxV = safeInt(stats.maxValue());
            if(max<maxV){
                max = maxV;
            }
        }

        @Override public long minCount() { return minCount; }
        @Override public ColumnStatistics<DataValueDescriptor> getClone() { return new IntAverage(columnId); }
        @Override public FrequentElements<DataValueDescriptor> topK() { return empty; }

        @Override public DataValueDescriptor minValue() { return new SQLInteger(min); }
        @Override public DataValueDescriptor maxValue() { return new SQLInteger(max); }


        @Override
        public Distribution<DataValueDescriptor> getDistribution() {
            IntColumnStatistics scs = new IntColumnStatistics(columnId,
                    FixedCardinalityEstimate.intEstimate(cardinality()),
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

        private int safeInt(DataValueDescriptor min)  {
            try {
                return min.getInt();
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class LongAverage extends ColumnAverage{
        private long min;
        private long minCount;
        private long max;
        private final FrequentElements<DataValueDescriptor> empty;

        public LongAverage(int columnId) {
            super(columnId);
            this.empty = new BigintStats.LongFreqs(LongFrequentElements.topK(0,0, Collections.<LongFrequencyEstimate>emptyList()));
        }

        @Override
        protected void mergeExtrema(ColumnStatistics<DataValueDescriptor> stats) {
            DataValueDescriptor minDvd = stats.minValue();
            long minV = safeLong(minDvd);
            if(min>minV){
                min = minV;
                minCount = stats.minCount();
            }
            long maxV = safeLong(stats.maxValue());
            if(max<maxV){
                max = maxV;
            }
        }

        @Override public long minCount() { return minCount; }
        @Override public ColumnStatistics<DataValueDescriptor> getClone() { return new LongAverage(columnId); }
        @Override public FrequentElements<DataValueDescriptor> topK() { return empty; }

        @Override public DataValueDescriptor minValue() { return new SQLLongint(min); }
        @Override public DataValueDescriptor maxValue() { return new SQLLongint(max); }


        @Override
        public Distribution<DataValueDescriptor> getDistribution() {
            LongColumnStatistics scs = new LongColumnStatistics(columnId,
                    FixedCardinalityEstimate.longEstimate(cardinality()),
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

        private long safeLong(DataValueDescriptor min)  {
            try {
                return min.getLong();
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class FloatAverage extends ColumnAverage{
        private float min;
        private long minCount;
        private float max;
        private final FrequentElements<DataValueDescriptor> empty;

        public FloatAverage(int columnId) {
            super(columnId);
            this.empty = new RealStats.FloatFreqs(FloatFrequentElements.topK(0,0, Collections.<FloatFrequencyEstimate>emptyList()));
        }

        @Override
        protected void mergeExtrema(ColumnStatistics<DataValueDescriptor> stats) {
            DataValueDescriptor minDvd = stats.minValue();
            float minV = safeFloat(minDvd);
            if(min>minV){
                min = minV;
                minCount = stats.minCount();
            }
            float maxV = safeFloat(stats.maxValue());
            if(max<maxV){
                max = maxV;
            }
        }

        @Override public long minCount() { return minCount; }
        @Override public ColumnStatistics<DataValueDescriptor> getClone() { return new FloatAverage(columnId); }
        @Override public FrequentElements<DataValueDescriptor> topK() { return empty; }

        @Override public DataValueDescriptor minValue() {
            try {
                return new SQLReal(min);
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
        @Override public DataValueDescriptor maxValue() {
            try {
                return new SQLReal(max);
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }


        @Override
        public Distribution<DataValueDescriptor> getDistribution() {
            FloatColumnStatistics scs = new FloatColumnStatistics(columnId,
                    FixedCardinalityEstimate.floatEstimate(cardinality()),
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

        private float safeFloat(DataValueDescriptor min)  {
            try {
                return min.getFloat();
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class DoubleAverage extends ColumnAverage{
        private double min;
        private long minCount;
        private double max;
        private final FrequentElements<DataValueDescriptor> empty;

        public DoubleAverage(int columnId) {
            super(columnId);
            this.empty = new DoubleStats.DoubleFreqs(DoubleFrequentElements.topK(0,0, Collections.<DoubleFrequencyEstimate>emptyList()));
        }

        @Override
        protected void mergeExtrema(ColumnStatistics<DataValueDescriptor> stats) {
            DataValueDescriptor minDvd = stats.minValue();
            double minV = safeDouble(minDvd);
            if(min>minV){
                min = minV;
                minCount = stats.minCount();
            }
            double maxV = safeDouble(stats.maxValue());
            if(max<maxV){
                max = maxV;
            }
        }

        @Override public long minCount() { return minCount; }
        @Override public ColumnStatistics<DataValueDescriptor> getClone() { return new DoubleAverage(columnId); }
        @Override public FrequentElements<DataValueDescriptor> topK() { return empty; }

        @Override public DataValueDescriptor minValue() {
            try {
                return new SQLDouble(min);
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
        @Override public DataValueDescriptor maxValue() {
            try {
                return new SQLDouble(max);
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }


        @Override
        public Distribution<DataValueDescriptor> getDistribution() {
            DoubleColumnStatistics scs = new DoubleColumnStatistics(columnId,
                    FixedCardinalityEstimate.doubleEstimate(cardinality()),
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

        private double safeDouble(DataValueDescriptor min)  {
            try {
                return min.getDouble();
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class DecimalAverage extends ColumnAverage{
        private BigDecimal min;
        private long minCount;
        private BigDecimal max;
        private final FrequentElements<DataValueDescriptor> empty;

        public DecimalAverage(int columnId) {
            super(columnId);
            this.empty = new NumericStats.Freqs(ObjectFrequentElements.topK(0,0,
                    Collections.<FrequencyEstimate<BigDecimal>>emptyList(),ComparableComparator.<BigDecimal>newComparator()));
        }

        @Override
        protected void mergeExtrema(ColumnStatistics<DataValueDescriptor> stats) {
            DataValueDescriptor minDvd = stats.minValue();
            BigDecimal minV = safeDecimal(minDvd);
            if(min==null || min.compareTo(minV)>0){
                min = minV;
                minCount = stats.minCount();
            }
            BigDecimal maxV = safeDecimal(stats.maxValue());
            if(max==null||max.compareTo(maxV)<0){
                max = maxV;
            }
        }

        @Override public long minCount() { return minCount; }
        @Override public ColumnStatistics<DataValueDescriptor> getClone() { return new DecimalAverage(columnId); }
        @Override public FrequentElements<DataValueDescriptor> topK() { return empty; }

        @Override public DataValueDescriptor minValue() { return new SQLDecimal(min); }
        @Override public DataValueDescriptor maxValue() { return new SQLDecimal(max); }

        @Override
        public Distribution<DataValueDescriptor> getDistribution() {
            ObjectFrequentElements<BigDecimal> fe = ObjectFrequentElements.topK(0, 0,
                    Collections.<FrequencyEstimate<BigDecimal>>emptyList(), ComparableComparator.<BigDecimal>newComparator());
            ColumnStatistics<BigDecimal> scs = new ComparableColumnStatistics<>(columnId,
                    new FixedCardinalityEstimate<BigDecimal>(cardinality()),
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

        private BigDecimal safeDecimal(DataValueDescriptor dvd)  {
            try {
                return (BigDecimal)dvd.getObject();
            } catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private abstract static class StringAverage extends ColumnAverage{
        private String min;
        private long minCount;
        private String max;
        private final FrequentElements<DataValueDescriptor> empty;
        protected int maxLength;

        public StringAverage(int columnId,int maxLength,
                             Function<FrequencyEstimate<String>,FrequencyEstimate<DataValueDescriptor>> conversionFunction) {
            super(columnId);
            FrequentElements<String> empty = ObjectFrequentElements.topK(0,0,
                    Collections.<FrequencyEstimate<String>>emptyList(),ComparableComparator.<String>newComparator());
            this.empty = new StringStatistics.Freqs(empty,conversionFunction);
            this.maxLength = maxLength;
        }

        @Override
        protected void mergeExtrema(ColumnStatistics<DataValueDescriptor> stats) {
            DataValueDescriptor minDvd = stats.minValue();
            String minV = safeString(minDvd);
            if(min==null||min.compareTo(minV)>0){
                min = minV;
                minCount = stats.minCount();
            }
            String maxV = safeString(stats.maxValue());
            if(max==null||max.compareTo(maxV)<0){
                max = maxV;
            }
        }

        @Override public long minCount() { return minCount; }
        @Override public FrequentElements<DataValueDescriptor> topK() { return empty; }

        @Override public DataValueDescriptor maxValue() { return getDvd(max); }
        @Override public DataValueDescriptor minValue() { return getDvd(min); }

        @Override
        public Distribution<DataValueDescriptor> getDistribution() {
            ObjectFrequentElements<String> fe = ObjectFrequentElements.topK(0, 0,
                    Collections.<FrequencyEstimate<String>>emptyList(),
                    ComparableComparator.<String>newComparator());
            ColumnStatistics<String> scs = new ComparableColumnStatistics<>(columnId,
                    new FixedCardinalityEstimate<String>(cardinality()),
                    fe,
                    min,
                    max,
                    totalBytes(),
                    nonNullCount()+nullCount(),
                    nullCount(),
                    minCount,DvdStatsCollector.stringDistributionFactory(maxLength)
            );
            return new StringStatistics.StringDistribution(scs);
        }

        protected abstract DataValueDescriptor getDvd(String value);

        private String safeString(DataValueDescriptor min)  {
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
        @Override public ColumnStatistics<DataValueDescriptor> getClone() {return new CharAverage(columnId,maxLength);}
    }

    private static class VarcharAverage extends StringAverage{

        public VarcharAverage(int columnId, int maxLength) { super(columnId, maxLength,VarcharStats.conversionFunction); }
        @Override protected DataValueDescriptor getDvd(String value) { return new SQLVarchar(value); }

        @Override public ColumnStatistics<DataValueDescriptor> getClone() {
            return new VarcharAverage(columnId,maxLength);
        }
    }
}
