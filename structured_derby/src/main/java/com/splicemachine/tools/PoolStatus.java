package com.splicemachine.tools;

import javax.management.MXBean;

/**
 * @author Scott Fines
 * Created on: 3/22/13
 */
@MXBean
public interface PoolStatus {

    int getWaiting();

    int getMaxPoolSize();

    void setMaxPoolSize(int newMaxPoolSize);

    int getAvailable();

    int getInUse();
}
