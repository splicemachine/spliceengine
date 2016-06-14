package com.splicemachine.metrics;

/**
 * @author Scott Fines
 *         Date: 12/19/14
 */
public interface Stats {
    TimeView getTime();

    long elementsSeen();
}
