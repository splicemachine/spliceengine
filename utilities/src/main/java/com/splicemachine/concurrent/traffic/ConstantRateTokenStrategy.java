/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.concurrent.traffic;

import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 11/13/14
 */
class ConstantRateTokenStrategy implements TokenBucket.TokenStrategy{
    private final TimeUnit timeUnit;
    /*The number of tokens to generate for each unit of time*/
    private final int tokensPerUnitTime;

    private final long minWaitTimeNanos; //cached for performance
    private final double tokensPerMs;

    public ConstantRateTokenStrategy(int tokensPerUnitTime,TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
        this.tokensPerUnitTime = tokensPerUnitTime;
        /*
         * The minimum wait time is the time it takes to acquire at least one
         * token. So we want to know how many nanoseconds it is required to wait
         * until 1 token is allowed.
         */
        this.minWaitTimeNanos = timeUnit.toNanos(1l)/tokensPerUnitTime;
        this.tokensPerMs = (double)tokensPerUnitTime/timeUnit.toMillis(1l);
    }

    @Override
    public int getTokensAdded(long timeDiffMs) {
        long tokens = (long)(timeDiffMs*tokensPerMs);
        if(tokens>Integer.MAX_VALUE)
            return Integer.MAX_VALUE;
        return (int)tokens;
    }

    @Override
    public long estimateNanos(int tokensDesired) {
        /*
         * We generate N tokens/unit time, so first compute how many time units
         * we would need to generate that many tokens, and then convert to nanos.
         */
        int unitsToGenerateTokenCount = tokensDesired/tokensPerUnitTime;
        return timeUnit.toNanos(unitsToGenerateTokenCount);
    }

    @Override public long minWaitTimeNanos() { return minWaitTimeNanos; }
}
