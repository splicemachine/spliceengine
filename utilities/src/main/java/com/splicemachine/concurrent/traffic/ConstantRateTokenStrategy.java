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
