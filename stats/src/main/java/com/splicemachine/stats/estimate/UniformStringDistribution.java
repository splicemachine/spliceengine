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

package com.splicemachine.stats.estimate;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.stats.util.UTFUtils;
import com.splicemachine.utils.ComparableComparator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Set;

/**
 * A uniform distribution of strings.
 *
 * <p>
 *     In this implementation, all strings are assumed to occur with equal probability. That is,
 *      if {@code p(x)} is the probability that {@code x} is equal to a specific value, then this implementation
 *      assumes that {@code p(x) = P}, where {@code P} is a constant. Since all elements have an equal
 *      probability of occurring, and {@code N} is the total number of elements in the data set,
 *      then we would see that each element likely occurs about {@code P*N = Ns} times. Since each distinct element
 *      in the data set will occur {@code Ns} times, then the total number of rows in the data set should be {@code C*Ns},
 *      where {@code C} is the cardinality of the data set. Therefore, we see that {@code Ns = N/C}.
 * </p>
 *
 * <p>
 *     To estimate range scans, we simply estimate the number of elements which match each single row, and multiply
 *     that by the number of distinct elements which we expect to see within that range. To do this, we make use
 *     of the fact that, for any subrange, {@code N/C = Nr/Cr}, where {@code Cr} is the number of distinct elements
 *     in the range of interest, and {@code Nr} is the total number of elements in the range of interest. Therefore,
 *     if we can compute {@code Cr}, we can compute {@code Nr = Cr*N/C}, which will give us our estimate.
 * </p>
 *
 * <p>
 *     In order to compute {@code Cr}, we perform a simple linear interpolation. When we are at the minimum value,
 *     the cardinality of the data set is 0, and when we are at the maximum value, the cardinality of the data set
 *     is {@code C}. Therefore, our linear interpolation says that the cardinality for any range {@code [min,x)}
 *
 *     <pre>
 *         Cr(x) = C*d(min,x)/d(min,max)
 *     </pre>
 *     where {@code d(a,b)} is the number of distinct elements which can occur in the range {@code [a,b)}. Thus, for
 *     any range{@code [x,y)}, the number of distinct elements in that range is {@code C*d(x,y)/d(min,max) = Cr}.
 * </p>
 *
 * <p>
 *     Note that, for computing range scans, we <em>must</em> have a well-constructed distance function. We do this
 *     by using the "position" of a string in the total ordering defined for the column. By default, this implementation
 *     uses a lexicographic ordering, in which the position of the string is computed using the
 *     {@link #computePosition(String)} method (more details about the default implementation of that is in the javadoc
 *     for that method). The distance is then {@code Pos(y)-Pos(x)}(assuming {@code y>x}).
 * </p>
 *
 * @author Scott Fines
 *         Date: 3/5/15
 */
public class UniformStringDistribution extends UniformDistribution<String> {
    protected final int strLen;

    private final BigDecimal a;

    public UniformStringDistribution(ColumnStatistics<String> columnStats, int strLen) {
        super(columnStats, ComparableComparator.<String>newComparator());
        this.strLen = strLen;

        if (columnStats.maxValue() == null) {
            this.a = BigDecimal.ZERO;
        } else {
            String maxV = columnStats.maxValue();
            String minV = columnStats.minValue();
            if(maxV.equals(minV)){
                /*
                 * the start and stop values are the same, so we just assume a constant line
                 */
                this.a = BigDecimal.ZERO;
            }else{
                BigInteger maxPosition=computePosition(columnStats.maxValue());
                /*
                 * The linear function is only used for the uniform portion of the histogram, so we
                 * can't use the totalCount(), we have to adjust that count to account for frequent elements.
                 */
                BigDecimal at=BigDecimal.valueOf(getAdjustedRowCount()-columnStats.minCount());
                BigInteger overallDistance=maxPosition.subtract(computePosition(columnStats.minValue()));
                at=at.divide(new BigDecimal(overallDistance),MathContext.DECIMAL64);

                this.a=at;
            }
        }
    }

    @Override
    protected final long estimateRange(String start, String stop, boolean includeStart, boolean includeStop, boolean isMin) {
        BigInteger startPos = computePosition(start);
        BigInteger stopPos = computePosition(stop);
        BigDecimal dist = new BigDecimal(stopPos.subtract(startPos));

        BigDecimal baseE = a.multiply(dist);

        /*
         * This is safe, because the linear function we used has a max of maxValue on the range [minValue,maxValue),
         * with a maximum value of rowCount (we built the linear function to do this). Since rowCount is a long,
         * the value of baseE *MUST* fit within a long (and therefore, within a double).
         */
        double baseEstimate = baseE.doubleValue();


        FrequentElements<String> fe = columnStats.topK();
        //if we are the min value, don't include the start key in frequent elements
        boolean includeMinFreq = includeStart &&!isMin;
        Set<? extends FrequencyEstimate<String>> estimates = fe.frequentElementsBetween(start,stop,includeMinFreq,includeStop);
        long l=uniformRangeCount(includeMinFreq,includeStop,baseEstimate,estimates);
        if(isMin&&includeStart)
            l+=minCount();
        return l;
    }

    public long cardinality(String start,String stop,boolean includeStart,boolean includeStop){
        if(start==null){
            start = minValue();
            includeStart = true;
        }
        if(stop==null){
            stop=maxValue();
            includeStart = true;
        }

        BigInteger s = computePosition(start);
        BigInteger e = computePosition(stop);

        BigInteger c = e.subtract(s);
        if(!includeStart) c = c.subtract(BigInteger.ONE);
        if(includeStop) c = c.add(BigInteger.ONE);
        return c.longValue();
    }

    @Override public String minValue(){ return columnStats.minValue(); }
    @Override public long minCount(){ return columnStats.minCount(); }
    @Override public String maxValue(){ return columnStats.maxValue(); }
    @Override public long totalCount(){ return columnStats.nonNullCount(); }
    public long cardinality(){ return columnStats.cardinality(); }

    /**
     * Compute the "position" of the specified string, relative to the total ordering of the defined string
     * set.
     *
     * <p>
     *     The default algorithm uses the recurrance relation
     *
     *    <pre>
     *       {@code pos(i,s) = m*pos(i-1,s) + s[i]}
     *       {@code pos(0,s) = s[0]}
     *    </pre>
     *
     *    where {@code m = 2^32}, and {@code s[i]} is the Unicode code point for the {@code i}th character in the
     *    string. When the length of the string is strictly less than the maximum length, we "pad" the string with
     *    0s until the length is equal. We do not physically add characters to the string, however; instead, we
     *    simply scale the number by {@code m^(strLen-length(s))} to represent the extra padding.
     * </p>
     *
     * <p>
     *     It is acknowledged that this approach may not be ideal for many use cases (particularly ones in which
     *     there are only a few possible elements for each character). Thus, this method is designed to be
     *     subclassed, so that different methods may be used when necessary.
     * </p>
     * @param str the the string to compute the position for.
     * @return a numeric representation of the location of the string within the total ordering of all possible
     * strings.
     */
    protected BigInteger computePosition(String str) {
        return UTFUtils.utf8Position(str,strLen);
    }

    /**
     * Get a Distribution Factory which creates uniform distributions for strings, based on the <em>maximum</em>
     * length of the string field allowed.
     *
     * <p>
     *     Note that this implementation depends heavily on there <em>being</em> a maximum string length (otherwise,
     *     it is unable to compute the distance between two strings effectively). If you have a situation where
     *     you cannot rely on that fact, you are best off subclassing this implementation and overriding
     *     {@link #computePosition(String)} in order to deal with your particular environment.
     * </p>
     * @param stringLength the maximum length of a string which is part of this distribution
     * @return a DistributionFactory which creates Uniform distributions.
     */
    public static DistributionFactory<String> factory(final int stringLength){
        return new DistributionFactory<String>() {
            @Override
            public Distribution<String> newDistribution(ColumnStatistics<String> statistics) {
                return new UniformStringDistribution(statistics,stringLength);
            }
        };
    }
}
