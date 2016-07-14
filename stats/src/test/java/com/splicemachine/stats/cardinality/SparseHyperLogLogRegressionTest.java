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

import org.junit.Test;

import java.sql.Time;

import static org.junit.Assert.*;

/**
 * Includes tests for specific regression scenarios encountered.
 *
 * @author Scott Fines
 *         Date: 8/4/15
 */
public class SparseHyperLogLogRegressionTest{

    @Test
    public void testCardinalityAccurateForTimeValues() throws Exception{
        /*
         * Regression test for DB-3474. Ensures that a low cardinality column is
         * correct over a small data range (e.g. it stays sparse the entire time).
         */
        @SuppressWarnings("deprecation") Time t = new Time(8,56,6);
        int p = 14;
        int size = 33;
        LongCardinalityEstimator lce = CardinalityEstimators.hyperLogLogLong(p);
        int repeatThreshold = 6000/size;
        for(int i=0;i<size;i++){
            for(int j=0;j<repeatThreshold;j++){
                lce.update(t.getTime());
            }
            t = new Time(t.getTime()+1000); //move forward by 1 second
        }
        assertEquals("Incorrect cardinality!",size,lce.getEstimate());
    }

    @Test
    public void testCardinalityAccurateForUniqueValues() throws Exception{
        /*
         * Regression test for DB-3474. Ensures that a primary key-type column is
         * correct over a small data range (i.e. it stays sparse the entire time).
         */
        int size = 6000;
        int p = 14;
        LongCardinalityEstimator lce = CardinalityEstimators.hyperLogLogLong(p);
        for(int i=0;i<size;i++){
            lce.update(i);
        }
        assertEquals("Incorrect cardinality!",size,lce.getEstimate());
    }
}