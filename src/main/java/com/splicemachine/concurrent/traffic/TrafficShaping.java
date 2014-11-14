package com.splicemachine.concurrent.traffic;

import java.util.concurrent.TimeUnit;

/**
 * Utility class for generating Traffic shapers.
 *
 * @author Scott Fines
 *         Date: 11/13/14
 */
public class TrafficShaping {

    private TrafficShaping(){}

    public static TrafficController fixedRateTrafficShaper(int maxInstantaneousPermits,
                                                       int permitsPerUnitTime,TimeUnit timeUnit){
        TokenBucket.TokenStrategy tokenStrategy = new ConstantRateTokenStrategy(permitsPerUnitTime,timeUnit);
        return new TokenBucket(maxInstantaneousPermits,tokenStrategy,new RetryBackoffWaitStrategy(1024));
    }
}
