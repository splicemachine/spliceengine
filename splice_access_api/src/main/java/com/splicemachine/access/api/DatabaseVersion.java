/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
     * @return true if we were unable to determine the version
     */
    boolean isUnknown();
}
