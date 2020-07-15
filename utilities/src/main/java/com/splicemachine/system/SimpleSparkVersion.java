/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.google.common.base.Splitter;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static org.apache.commons.lang3.StringUtils.isBlank;


/**
 * Represents a Spark version string and provides access to its components.
 *
 * Expects three version components separated by a period, each of which contains numeric
 * major.minor.patch version numbers, plus optional additional version components which
 * may describe the version of the hadoop platform.
 * Examples:
 *
 * 2.2.0
 * 2.3.1
 * 2.3.0.2.6.5.0-292
 * 2.2.0.cloudera2
 *
 * The major.minor.patch version numbers are the only parts used to
 * determine the spark version.  The rest of the version string is
 * kept for display purposes, but ignored when comparing two
 * SparkVersion objects.
 */
public class SimpleSparkVersion implements SparkVersion {

    private   static final Splitter SPLITTER = Splitter.on(".");
    protected static final String UNKNOWN_VERSION = "UNKNOWN";
    protected static final int UNKNOWN_INT = -1;

    private final String sparkVersion;
    private final int majorVersion;
    private final int minorVersion;
    private final int patchVersion;

    public SimpleSparkVersion(Map<String, String> manifestProps) {
        if (manifestProps == null) {
            sparkVersion = "";
            majorVersion = UNKNOWN_INT;
            minorVersion = UNKNOWN_INT;
            patchVersion = UNKNOWN_INT;
        }
        else {
            sparkVersion = safeGet(manifestProps, "Spark-Version");
            Iterator<String> versionParts = SPLITTER.split(sparkVersion).iterator();
            majorVersion = versionParts.hasNext() ? safeParseInt(versionParts.next()) : UNKNOWN_INT;
            minorVersion = versionParts.hasNext() ? safeParseInt(versionParts.next()) : UNKNOWN_INT;
            patchVersion = versionParts.hasNext() ? safeParseInt(versionParts.next()) : UNKNOWN_INT;
        }
    }

    public SimpleSparkVersion(String sparkVersion) {
        if (sparkVersion == null) {
            this.sparkVersion = "";
            majorVersion = UNKNOWN_INT;
            minorVersion = UNKNOWN_INT;
            patchVersion = UNKNOWN_INT;
        }
        else {
            this.sparkVersion = sparkVersion;
            Iterator<String> versionParts = SPLITTER.split(sparkVersion).iterator();
            majorVersion = versionParts.hasNext() ? safeParseInt(versionParts.next()) : UNKNOWN_INT;
            minorVersion = versionParts.hasNext() ? safeParseInt(versionParts.next()) : UNKNOWN_INT;
            patchVersion = versionParts.hasNext() ? safeParseInt(versionParts.next()) : UNKNOWN_INT;
        }
    }

    @Override
    public boolean greaterThanOrEqualTo(SparkVersion otherValue) {
        if (this.getMajorVersionNumber() > otherValue.getMajorVersionNumber())
            return true;
    	else if (this.getMajorVersionNumber() == otherValue.getMajorVersionNumber()) {
    	    if (this.getMinorVersionNumber() > otherValue.getMinorVersionNumber()) {
    	        return true;
            }
    	    else if (this.getMinorVersionNumber() == otherValue.getMinorVersionNumber()) {
    	        if (this.getPatchVersionNumber() >= otherValue.getPatchVersionNumber()) {
    	            return true;
                }
    	        else
    	            return false;
            }
    	    else
    	        return false;
        }
    	else
    	    return false;
    }

    @Override
    public boolean lessThan(SparkVersion otherValue) {
        if (this.getMajorVersionNumber() < otherValue.getMajorVersionNumber())
            return true;
    	else if (this.getMajorVersionNumber() == otherValue.getMajorVersionNumber()) {
    	    if (this.getMinorVersionNumber() < otherValue.getMinorVersionNumber()) {
    	        return true;
            }
    	    else if (this.getMinorVersionNumber() == otherValue.getMinorVersionNumber()) {
    	        if (this.getPatchVersionNumber() < otherValue.getPatchVersionNumber()) {
    	            return true;
                }
    	        else
    	            return false;
            }
    	    else
    	        return false;
        }
    	else
    	    return false;
    }

    @Override
    public boolean equals(SparkVersion otherValue) {
        return (this.getMajorVersionNumber() == otherValue.getMajorVersionNumber() &&
                this.getMinorVersionNumber() == otherValue.getMinorVersionNumber() &&
                this.getPatchVersionNumber() == otherValue.getPatchVersionNumber());
    }

    public String toString() {
        return getVersionString();
    }

    @Override
    public String getVersionString() {
        return sparkVersion;
    }

    @Override
    public int getMajorVersionNumber() {
        return majorVersion;
    }

    @Override
    public int getMinorVersionNumber() {
        return minorVersion;
    }

    @Override
    public int getPatchVersionNumber() {
        return patchVersion;
    }

    @Override
    public boolean isUnknown() {
        return majorVersion == UNKNOWN_INT || minorVersion == UNKNOWN_INT || patchVersion == UNKNOWN_INT;
    }

    private static String safeGet(Map<String, String> map, String propertyName) {
        if (map == null) {
            return UNKNOWN_VERSION;
        }
        String value = map.get(propertyName);
        return isBlank(value) ? UNKNOWN_VERSION : value;
    }

    private static int safeParseInt(String intString) {
        if (isBlank(intString)) {
            return UNKNOWN_INT;
        }
        try {
            Matcher matcher = Pattern.compile("(\\d+)").matcher(intString.trim());
            return matcher.find() ? Integer.parseInt(matcher.group()) : UNKNOWN_INT;
        } catch (NumberFormatException nfe) {
            return UNKNOWN_INT;
        }
    }

}
