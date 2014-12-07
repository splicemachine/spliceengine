package com.splicemachine.stats.frequency;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public interface DoubleFrequencyEstimate extends FrequencyEstimate<Double> {

    double value();
}
