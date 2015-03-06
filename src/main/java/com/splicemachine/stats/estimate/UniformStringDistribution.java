package com.splicemachine.stats.estimate;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.frequency.FrequencyEstimate;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.utils.ComparableComparator;

import java.math.BigDecimal;
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
public class UniformStringDistribution extends BaseDistribution<String> {
    protected final int strLen;

    public UniformStringDistribution(ColumnStatistics<String> columnStats, int strLen) {
        super(columnStats, ComparableComparator.<String>newComparator());
        this.strLen = strLen;
    }

    @Override
    protected final long estimateRange(String start, String stop, boolean includeStart, boolean includeStop, boolean isMin) {
        BigDecimal startPos = computePosition(start);
        BigDecimal stopPos = computePosition(stop);

        BigDecimal dist = stopPos.subtract(startPos);
        BigDecimal maxDist = computePosition(columnStats.maxValue()).subtract(computePosition(columnStats.minValue()));
        BigDecimal scale = dist.divide(maxDist, MathContext.DECIMAL64);

        long actualCardinality = columnStats.cardinality();
        BigDecimal adjCard = scale.multiply(BigDecimal.valueOf(actualCardinality));
        //since dist<maxDist, adjCard < cardinality, so it fits in a long
        long countPerEntry = getAdjustedRowCount()/actualCardinality;
        long baseEstimate = adjCard.longValue()*countPerEntry;

        if(!includeStart){
            baseEstimate-=countPerEntry;
        }else if(isMin){
            baseEstimate-=countPerEntry;
            baseEstimate+=columnStats.minCount();
        }
        if(includeStop)
            baseEstimate+=countPerEntry;

        FrequentElements<String> fe = columnStats.topK();
        Set<? extends FrequencyEstimate<String>> estimates = fe.frequentElementsBetween(start, stop, includeStart, includeStop);
        baseEstimate-=countPerEntry*estimates.size();
        for(FrequencyEstimate<String> est:estimates){
            baseEstimate+=est.count();
        }
        return baseEstimate;
    }

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
    protected BigDecimal computePosition(String str) {
        return unicodePosition(strLen,str);
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

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static final BigDecimal TWO_THIRTY_TWO = BigDecimal.valueOf(Integer.MAX_VALUE);
    private static BigDecimal unicodePosition(int strLen,String str) {
        /*
         * Each UTF-8 encoded character takes up between 1 and 4 bytes, so if we represent each character
         * with a single big-endian integer holding the character, then we can compute the absolute position
         * in the ordering according to the rule
         *
         * 2^strLen*char[0]+2^(strLen-1)*char[1]+...char[strLen-1]
         */
        int len = str.length();

        BigDecimal pos = BigDecimal.ZERO;
        for(int codePoint = 0;codePoint<len;codePoint++){
            int v = str.codePointAt(codePoint);
            pos = BigDecimal.valueOf(v).add(TWO_THIRTY_TWO.multiply(pos));
        }

        //now shift the data over to account for string padding at the end
        if(strLen>len){
            BigDecimal scale = TWO_THIRTY_TWO.pow(strLen-len);
            pos = pos.multiply(scale);
        }
        return pos;
    }
}
