package com.splicemachine.derby.stats;

import java.io.Externalizable;

/**
 * @author Scott Fines
 *         Created on: 2/26/13
 */
public interface Stats extends Externalizable{

    /**
     * @return the total time taken to process all records.
     */
    public long getTotalTime();

    /**
     * @return the total number of processed records
     */
    public long getTotalRecords();

    /**
     * @return the average time taken to process a single record
     */
    public double getAvgTime();

    /**
     * @return the standard deviation in processing time for a single record.
     */
    public double getTimeStandardDeviation();

    /**
     * @return the maximum processing time for a single record.
     */
    public long getMaxTime();

    /**
     * @return the minimum processing time for a single record.
     */
    public long getMinTime();

    /**
     * @return the median time taken to process a single record.
     */
    public double getMedian();

    /**
     * @return the 75th percentile time taken to process a single record. That is,
     * 75% of all records were processed in less time than this.
     */
    public double get75P();

    /**
     * @return the 95th percentile time taken to process a single record. That is,
     * 95% of all records were processed in less time than this.
     */
    public double get95P();

    /**
     * @return the 98th percentile time taken to process a single record. That is,
     * 98% of all records were processed in less time than this.
     */
    public double get98P();

    /**
     * @return the 99th percentile time taken to process a single record. That is,
     * 99% of all records were processed in less time than this.
     */
    public double get99P();

    /**
     * @return the 99.9th percentile time taken to process a single record. That is,
     * 99.9% of all records were processed in less time than this.
     */
    public double get999P();
}
