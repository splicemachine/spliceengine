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

package com.splicemachine.access.api;

import javax.management.MXBean;

/**
 * Interface to expose to JMX for version info
 */
@MXBean
public interface DatabaseVersion{

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
     * @return the Splice sprint number
     */
    int getSprintVersionNumber();

    /**
     * @return true if we were unable to determine the version
     */
    boolean isUnknown();
}
