package com.splicemachine.testutil;

import com.google.common.collect.Lists;
import org.apache.commons.lang.RandomStringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A builder for building collections of BigDecimals with that fit in derby's decimal column.
 * <p/>
 * <pre>
 *
 * List<BigDecimal> derbyDecimals = new RandomDerbyDecimalBuilder()
 *                                  .withNegatives(true).build(1000);
 *
 * </pre>
 */
public class RandomDerbyDecimalBuilder {

    private static final Random RANDOM = new Random();
    private static int PRECISION_MIN = 1;
    private static int PRECISION_MAX = 31;

    private static boolean DEFAULT_INCLUDE_NEGATIVES = true;

    private boolean includeNegatives = DEFAULT_INCLUDE_NEGATIVES;

    public RandomDerbyDecimalBuilder withNegatives(boolean includeNegatives) {
        this.includeNegatives = includeNegatives;
        return this;
    }

    public List<BigDecimal> build(int howMany) {
        List<BigDecimal> values = Lists.newArrayListWithCapacity(howMany);

        for (int i = 0; i < howMany; i++) {

            int derbyPrecision = 1 + RANDOM.nextInt(31);
            int derbyScale = RANDOM.nextInt(derbyPrecision + 1);
            boolean negative = includeNegatives ? RANDOM.nextBoolean() : false;

            values.add(buildOne(derbyPrecision, derbyScale, negative));
        }
        return values;
    }

    public BigDecimal[] buildArray(int howMany) {
        List<BigDecimal> list = build(howMany);
        return list.toArray(new BigDecimal[howMany]);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public static BigDecimal buildOne(int precision, int scale, boolean negative) {
        checkArgument(precision >= PRECISION_MIN && precision <= PRECISION_MAX);
        checkArgument(scale <= precision);

        BigInteger unscaledVal = new BigInteger((negative ? "-" : "") + RandomStringUtils.randomNumeric(precision));
        return new BigDecimal(unscaledVal, scale);
    }


}
