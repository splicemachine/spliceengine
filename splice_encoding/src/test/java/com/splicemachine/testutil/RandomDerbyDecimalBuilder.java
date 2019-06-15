/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.testutil;

import com.splicemachine.db.iapi.reference.Limits;
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
    private static int PRECISION_MAX = Limits.DB2_MAX_DECIMAL_PRECISION_SCALE;

    private static boolean DEFAULT_INCLUDE_NEGATIVES = true;

    private boolean includeNegatives = DEFAULT_INCLUDE_NEGATIVES;

    public RandomDerbyDecimalBuilder withNegatives(boolean includeNegatives) {
        this.includeNegatives = includeNegatives;
        return this;
    }

    public List<BigDecimal> build(int howMany) {
        List<BigDecimal> values = Lists.newArrayListWithCapacity(howMany);

        for (int i = 0; i < howMany; i++) {

            int derbyPrecision = 1 + RANDOM.nextInt(PRECISION_MAX);
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
