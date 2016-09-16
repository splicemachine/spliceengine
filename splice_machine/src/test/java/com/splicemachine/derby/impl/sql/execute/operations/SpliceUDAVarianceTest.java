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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ArchitectureIndependent.class)
public class SpliceUDAVarianceTest {
    @Test
    public void varianceSmallDataset() {
        SpliceStddevSamp<Double> stddevSamp = new SpliceStddevSamp<Double>();
        for (Double val : new double[]{4, 7, 13, 16}) {
            stddevSamp.accumulate(val);
        }
        double result = stddevSamp.terminate();
        Assert.assertEquals(Math.sqrt(30), result, 0.00001);
    }

    @Test
    public void testNumericalAccuracy() {
        SpliceStddevSamp<Double> stddevSamp;
        for (Double constant : new double[]{0, 1E4, 1E6, 1E8, 1E10, 1E12}) {
            double constantPlus0 =constant+0;
            double constantPlus1 =constant+1;
            stddevSamp =new SpliceStddevSamp<>();
            for (int i = 0; i < 10000000; ++i) {
                stddevSamp.accumulate(constantPlus0);
                stddevSamp.accumulate(constantPlus1);
            }
            double result = stddevSamp.terminate();
            Assert.assertEquals("Poor numerical accuracy for constant " + constant, .5, result, 0.00001);
        }
    }

    @Test
    public void testNumericalAccuracyStableVariance() {
        SpliceUDAStableVariance<Double> variance;
        for (Double constant : new double[]{0, 1E4, 1E6, 1E8, 1E10, 1E12}) {
            double constantPlus0 =constant+0;
            double constantPlus1 =constant+1;
            variance =new SpliceUDAStableVariance<>();
            for (int i = 0; i < 10000000; ++i) {
                variance.accumulate(constantPlus0);
                variance.accumulate(constantPlus1);
            }
            double stdDev = Math.sqrt(variance.terminate());
            Assert.assertEquals("Poor numerical accuracy for constant " + constant, .5, stdDev, 0.00001);
        }
    }
}

