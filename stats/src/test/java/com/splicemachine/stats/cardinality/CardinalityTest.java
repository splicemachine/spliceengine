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

package com.splicemachine.stats.cardinality;

import com.carrotsearch.hppc.IntOpenHashSet;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.stats.IntUpdateable;

import java.util.Random;

/**
 * @author Scott Fines
 *         Date: 3/27/14
 */
public class CardinalityTest{

    private CardinalityTest(){ }

    /**
     * @param estimator           the estimator to use
     * @param numElements         the total number of elements to generate
     * @param numDistinctElements the number of distinct elements to generate
     * @return the relative error in the estimate. relative error = Math.abs(actual-expected)/expected
     */
    public static double test(IntCardinalityEstimator estimator,int numElements,int numDistinctElements,Random random){
        IntOpenHashSet actualDistinct=new IntOpenHashSet(numDistinctElements);
        fill(estimator,numElements,numDistinctElements,random,actualDistinct);

        long cardinalityEstimate=estimator.getEstimate();
        long actualDistinctCount=actualDistinct.size();
        long absoluteError=Math.abs(cardinalityEstimate-actualDistinctCount);
        return ((double)absoluteError)/actualDistinctCount;
    }

    protected static void fill(IntUpdateable estimator,int numElements,int numDistinctElements,Random random,IntOpenHashSet actualDistinct){
        int numElementsToFill=random.nextInt(numElements);
        for(int i=0;i<numElementsToFill;i++){
            int next=i%numDistinctElements;

//            if(!actualDistinct.contains(next))
                actualDistinct.add(next);
            estimator.update(next);
        }
    }
}
