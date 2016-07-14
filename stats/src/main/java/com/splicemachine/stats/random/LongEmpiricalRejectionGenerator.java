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

package com.splicemachine.stats.random;

import com.splicemachine.stats.estimate.LongDistribution;

/**
 * Generates a sequence of random numbers according to an arbitrary distribution
 * @author Scott Fines
 *         Date: 7/2/15
 */
public class LongEmpiricalRejectionGenerator implements RandomGenerator{
    private final LongDistribution distribution;
    private final RandomGenerator  uniformGenerator;

    public LongEmpiricalRejectionGenerator(LongDistribution distribution,RandomGenerator rng){
        this.distribution=distribution;
        this.uniformGenerator=rng;
    }

    @Override
    public double nextDouble(){
        return nextLong()/((double)distribution.totalCount());
    }

    @Override
    public int nextInt(){
        long n = nextLong();
        return (int)n;
    }

    @Override
    public boolean nextBoolean(){
        //TODO -sf- cache this?
        double midPoint = (distribution.max()+distribution.min())/2d;
        return nextLong()>midPoint;
    }

    @Override
    public long nextLong(){
        while(true){
            double x=uniformGenerator.nextDouble(); //uniform in [0,1)
            x=x*(distribution.max()-distribution.min())+distribution.min(); //uniform in [min,max)
            long xl = Math.round(x);
            double y=uniformGenerator.nextDouble();
            double prob=distribution.selectivity(xl)/((double)distribution.totalCount());
            if(y<prob) return xl;
        }
    }
}
