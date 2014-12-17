package com.splicemachine.tools.version;

import javax.management.MXBean;

/**
 * Interface to expose to JMX for version info
 */
@MXBean
public interface SpliceMachineVersion {

    /**
     * @return the Splice release version
     */
    String getRelease();

    /**
     * @return the Git commit that was used to build this version
     */
    String getImplementationVersion();

    /**
     * @return the date/time of the build
     */
    String getBuildTime();

    /**
     * @return the Splice Machine URL
     */
    String getURL();

    /**
     * @return the Splice major release version
     */
    int getMajorVersionNumber();

    /**
     * @return the Splice minor release version
     */
    int getMinorVersionNumber();

    /**
     * @return the Splice patch number
     */
    int getPatchVersionNumber();

    /**
     * @return true if we were unable to determine the version
     */
    boolean isUnknown();
}
