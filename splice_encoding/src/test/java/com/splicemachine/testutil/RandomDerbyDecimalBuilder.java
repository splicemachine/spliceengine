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

package com.splicemachine.testutil;

import org.spark_project.guava.collect.Lists;
import org.apache.commons.lang.RandomStringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Random;
import static org.spark_project.guava.base.Preconditions.checkArgument;

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
