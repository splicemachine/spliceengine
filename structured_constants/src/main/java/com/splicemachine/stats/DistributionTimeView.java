package com.splicemachine.stats;

/**
 * Represents a time view which also contains distribution information
 * @author Scott Fines
 * Date: 7/10/14
 */
public interface DistributionTimeView extends TimeView {

    double getAverageWallTime();
    long getMedianWallTime();
    long getWallTime75p();
    long getWallTime90p();
    long getWallTime95p();
    long getWallTime99p();

    double getAverageCpuTime();
    long getMedianCpuTime();
    long getCpuTime75p();
    long getCpuTime90p();
    long getCpuTime95p();
    long getCpuTime99p();

    double getAverageUserTime();
    long getMedianUserTime();
    long getUserTime75p();
    long getUserTime90p();
    long getUserTime95p();
    long getUserTime99p();

}
