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

import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.SystemClock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Traffic-shaping CongestionControl mechanism using the "Token Bucket" algorithm.
 *
 * The Token bucket algorithm is analogized as follows. Imagine a bucket of fixed size,
 * into which is added 1 token per unit of time(up to some maximum number of tokens). When
 * an action wishes, it may attempt to take {@code N} tokens from the bucket. If there are
 * already {@code >= N} tokens in the bucket, then the action is considered conforming; it takes
 * the {@code N} tokens and returns immediately. Otherwise, the action is considered either
 * non-conforming (or partially conforming). If it is non-conforming, then it is forced to wait
 * until {@code N} tokens are made available.
 *
 * this implementation does not add 1 token per unit--intead, it adds {@code p} tokens every millisecond,
 * where {@code p} is the number of tokens that would have been added in that time. So, if the token addition
 * rate is 1 token per microsecond, then 1000 tokens will be added every millisecond. If, on the other hand,
 * the addition rate is 1 token per second, then 1 token will be added every 1000 milliseconds.
 *
 * @author Scott Fines
 *         Date: 11/13/14
 */
public class TokenBucket implements TrafficController {
    //a fuzzy sense of the last time the tokens were added.
    private final TokenStrategy tokenAdder;
    private AtomicInteger numTokens;
    private volatile AtomicLong fuzzyTime;
    private volatile int maxTokens;
    private final WaitStrategy waitStrategy;
    private final Clock clock;

    public TokenBucket(int maxTokens,TokenStrategy tokenStrategy, WaitStrategy waitStrategy) {
        this(maxTokens,tokenStrategy,waitStrategy,SystemClock.INSTANCE);
    }

    public TokenBucket(int maxTokens,TokenStrategy tokenStrategy, WaitStrategy waitStrategy,Clock clock) {
        this.waitStrategy = waitStrategy;
        this.tokenAdder = tokenStrategy;
        this.maxTokens = maxTokens;
        this.numTokens = new AtomicInteger(maxTokens);
        this.clock = clock;
        this.fuzzyTime = new AtomicLong(clock.currentTimeMillis());
    }

    @Override
    public TrafficStats stats() {
        return NoOpTrafficStats.INSTANCE;
    }

    @Override
    public void acquire(int numPermits) throws InterruptedException {
        tryAcquire(numPermits,Long.MAX_VALUE,TimeUnit.NANOSECONDS); //wait for forever
    }

    @Override
    public boolean tryAcquire(int numPermits) {
        return tryAcquire(numPermits,numPermits)==numPermits;
    }

    @Override
    public boolean tryAcquire(int numPermits, long timeout, TimeUnit timeUnit) throws InterruptedException {
        return acquire(numPermits,numPermits,timeout,timeUnit)==numPermits;
    }

    @Override
    public int acquire(int minPermits, int maxPermits, long timeout, TimeUnit timeUnit) throws InterruptedException {
        assert maxPermits>=minPermits: "maxPermits< minPermits";
        addNewTokens(); //get all your backfilled tokens

        /*
         * This implementation has a tendency to be overly conservative under high load--
         * that is, it is possible that it will cause others to be treated as non-conforming
         * even if they would not technically have been so. That is because this
         * will acquire permits in order,while waiting--the first round, it will acquire
         * as many as it can, then wait a bit, then acquire as many as it can, and so on and so
         * forth. Once acquired, the tokens aren't unacquired, so in the worst case, you
         * end up using up some tokens that could have been used by someone else to allow
         * writes through, only to end up being unable to use them because you can't acquire
         * up to your minimum.
         *
         * Still, you have two starvation choices--either you starve this thread, or you
         * start other theoretical threads. With this implementation, if you wait long enough
         * you are guaranteed to eventually get some resources--a more conservative approach (like
         * only trying to acquire permits once you *know* there will be enough) can result in
         * arbitrary wait times; in fact, even with a very high timeout, it would be possible
         * to starve a request of permits until it times out. The below implementation
         * will not have that problem, although it comes with a cost of rejecting more than it
         * might otherwise have done.
         */
        int iterationCount = 0;
        long timeLeftNanos = timeUnit.toNanos(timeout);
        int acquiredTokens = 0;
        int remaining = maxPermits;
        while(acquiredTokens<maxPermits && timeLeftNanos>0){
            if(Thread.currentThread().isInterrupted())
                throw new InterruptedException();

            int acquired = tryAcquire(0,remaining);
            acquiredTokens+=acquired;
            remaining-=acquired;
            if(remaining>0){
                timeLeftNanos -=timedWait(remaining,iterationCount,timeLeftNanos);
            }
            iterationCount++;
        }
        return acquiredTokens>=minPermits? acquiredTokens: 0;
    }

    @Override
    public int tryAcquire(int minPermits, int maxPermits) {
        if(minPermits>maxTokens) return 0; //you can never get that many tokens
        addNewTokens();

        boolean shouldContinue;
        int acquired = 0;
        do{
            int available= numTokens.get();
            int newVal = available;
            if(available<minPermits){
                //we don't have enough immediately available to service the min, so do nothing
                return 0;
            }else if(available<maxPermits){
                //we can acquire somewhere in between the value
                newVal-=available; //try and get everything you can
                acquired+=available;
            }else{
                //we should have enough to acquire everything
                newVal -=maxPermits;
                acquired = maxPermits;
            }
            shouldContinue = !numTokens.compareAndSet(available,newVal);
        }while(shouldContinue);
        return acquired;
    }

    private long timedWait(int numToAcquire, int iterationCount, long timeLeftNanos) {
        long ts = clock.nanoTime();
        waitStrategy.wait(iterationCount,timeLeftNanos,(numToAcquire<10? numToAcquire: 10)*tokenAdder.minWaitTimeNanos());
        ts= clock.nanoTime()-ts;
        return ts;
    }

    private synchronized void addNewTokens(){
        long ct = clock.currentTimeMillis();
        long ft = fuzzyTime.get();
        if(ct<=ft) return; //nothing to do
        /*
         * Set the fuzzy time. This will force anyone who comes in
         * during the same millisecond to stop, because they won't have
         * anything to do. However, multiple simultaneous calls will
         * all set the fuzzy time to the same millisecond, so the follow-up
         * is to do a compare and swap to ensure that the sum comes out properly.
         *
         * To do this, we have multiple threads--some will be decrementing,
         * and some will be incrementing. Incrementing only happens inside this
         * method; this means that decrementers can safely take the number DOWN
         * before we add in our values, but if the number goes UP, then another thread
         * added time tokens, which should include ours. Thus, we fail if the number
         * increases over the iteration, but we continue if it decreases
         */
            if(!fuzzyTime.compareAndSet(ft,ct)) return;

            long toAdd = tokenAdder.getTokensAdded(Math.abs(ct - ft));
            int current = 0; //so that we try at least once
            boolean shouldContinue;
            do {
                int old = current;
                current = numTokens.get();
                if (old>0 && current > old) return; //someone else did our update for us
                //cast is safe because we are capping it to maxTokens, which is an int
                int newVal = (int) Math.min(current + toAdd, maxTokens);
                shouldContinue = !numTokens.compareAndSet(current, newVal);
            } while (shouldContinue);
    }

    @Override
    public int availablePermits() {
        addNewTokens();
        return numTokens.get();
    }

    @Override
    public int maxPermits() {
        return maxTokens;
    }

    @Override
    public void setMaxPermits(int newMaxPermits) {
        if(newMaxPermits<1) throw new IllegalArgumentException("Cannot set a max permits < 1");
        this.maxTokens = newMaxPermits;
    }

    public static interface TokenStrategy{
        /**
         * Get the number of tokens which can be added in the given time range.
         * @param timeDiffMs the time diff(in milliseconds);
         * @return the number of tokens that can be added in that time range.
         */
        int getTokensAdded(long timeDiffMs);

        /**
         * Estimate the minimum amount of time (in nanoseconds) that it will take before {@code tokensDesired}
         * can be acquired.
         * @param tokensDesired the number of tokens to acquire
         * @return an estimate of the <em>minimum</em> time (in nanoseconds) that it will take to acquire
         * {@code tokensDesired}
         */
        long estimateNanos(int tokensDesired);

        /**
         * @return the minimum time (in nanoseconds) that one must wait to acquire a new token
         */
        long minWaitTimeNanos();
    }

}
