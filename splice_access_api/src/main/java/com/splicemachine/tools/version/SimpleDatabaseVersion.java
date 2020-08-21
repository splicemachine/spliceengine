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

package com.splicemachine.tools.version;

import com.splicemachine.access.api.DatabaseVersion;
import splice.com.google.common.base.Splitter;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Represents a version string and provides access to its components.
 *
 * Expects between zero and three version components separated by a period each of which contains some numeric
 * and (optionally) some non-numeric characters. Examples:
 *
 * 1.2.3
 * 1.2.3-SNAPSHOT
 * 1.2.3RC2
 * 1.2.3RC2-SNAPSHOT
 */
public class SimpleDatabaseVersion implements DatabaseVersion{

    private static final Splitter SPLITTER = Splitter.on(".");
    protected static final String UNKNOWN_VERSION = "UNKNOWN";
    protected static final int UNKNOWN_INT = -1;

    private final String release;
    private final String implementationVersion;
    private final String buildTime;
    private final String url;
    private final int majorVersion;
    private final int minorVersion;
    private final int patchVersion;
    private final int sprintVersion;

    SimpleDatabaseVersion(Map<String, String> manifestProps) {
        release = safeGet(manifestProps, "Release");
        implementationVersion = safeGet(manifestProps, "Implementation-Version");
        buildTime = safeGet(manifestProps, "Build-Time");
        url = safeGet(manifestProps, "URL");

        Iterator<String> versionParts = SPLITTER.split(release).iterator();
        majorVersion = versionParts.hasNext() ? safeParseInt(versionParts.next()) : UNKNOWN_INT;
        minorVersion = versionParts.hasNext() ? safeParseInt(versionParts.next()) : UNKNOWN_INT;
        patchVersion = versionParts.hasNext() ? safeParseInt(versionParts.next()) : UNKNOWN_INT;
        sprintVersion = versionParts.hasNext() ? safeParseInt(versionParts.next()) : 0;
    }

    @Override
    public String getRelease() {
        return release;
    }

    @Override
    public String getImplementationVersion() {
        return implementationVersion;
    }

    @Override
    public String getBuildTime() {
        return buildTime;
    }

    @Override
    public String getURL() {
        return url;
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
        return UNKNOWN_VERSION.equals(release);
    }

    private static String safeGet(Map<String, String> map, String propertyName) {
        if (map == null) {
            return UNKNOWN_VERSION;
        }
        String value = map.get(propertyName);
        return isBlank(value) ? UNKNOWN_VERSION : value;
    }

    private int safeParseInt(String intString) {
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

    @Override
    public int getSprintVersionNumber() {
        return sprintVersion;
    }

    /**
     * -sf- stolen directly from Apache commons-lang, but reproduced here to avoid
     * the dependency.
     *
     * <p>Checks if a String is whitespace, empty ("") or null.</p>
     *
     * <pre>
     * StringUtils.isBlank(null)      = true
     * StringUtils.isBlank("")        = true
     * StringUtils.isBlank(" ")       = true
     * StringUtils.isBlank("bob")     = false
     * StringUtils.isBlank("  bob  ") = false
     * </pre>
     *
     * @param str  the String to check, may be null
     * @return <code>true</code> if the String is null, empty or whitespace
     * @since 2.0
     */
    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if ((!Character.isWhitespace(str.charAt(i)))) {
                return false;
            }
        }
        return true;
    }
}
