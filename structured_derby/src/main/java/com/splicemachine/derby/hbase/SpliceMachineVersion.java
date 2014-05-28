package com.splicemachine.derby.hbase;

import javax.management.MXBean;

/**
 * @author Jeff Cunningham
 *         Date: 5/27/14
 */
@MXBean
public interface SpliceMachineVersion {

    String getRelease();

    String getImplementationVersion();

    String getBuildTime();

    String getURL();
}
