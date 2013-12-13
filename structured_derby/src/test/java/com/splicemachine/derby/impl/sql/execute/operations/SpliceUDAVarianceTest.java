package com.splicemachine.derby.impl.sql.execute.operations;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

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
    @Ignore
    public void testNumericalAccuracy() {
        SpliceStddevSamp<Double> stddevSamp;
        for (Double constant : new double[]{0, 1E4, 1E6, 1E8, 1E10, 1E12}) {
            Double constantPlus0 = new Double(constant + 0);
            Double constantPlus1 = new Double(constant + 1);
            stddevSamp = new SpliceStddevSamp<Double>();
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
            Double constantPlus0 = new Double(constant + 0);
            Double constantPlus1 = new Double(constant + 1);
            variance = new SpliceUDAStableVariance<Double>();
            for (int i = 0; i < 10000000; ++i) {
                variance.accumulate(constantPlus0);
                variance.accumulate(constantPlus1);
            }
            double stdDev = Math.sqrt(variance.terminate());
            Assert.assertEquals("Poor numerical accuracy for constant " + constant, .5, stdDev, 0.00001);
        }
    }
}

