/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.system;

import javax.management.MXBean;

/**
 * Interface to expose to JMX for Spark version info
 */
@MXBean
public interface SparkVersion{

    /**
     * @return the running version of splice, e.g. 2.2.0
     */
    String getVersionString();

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

    /**
     * @return true if this object's version is greater than
     * or equal to the other object's version.
     */
    public boolean greaterThanOrEqualTo(SparkVersion otherValue);

    /**
     * @return true if this object's version is less than
     * the other object's version.
     */
    public boolean lessThan(SparkVersion otherValue);

    /**
     * @return true if this object's version is equal to
     * the other object's version.
     */
    public boolean equals(SparkVersion otherValue);
}
