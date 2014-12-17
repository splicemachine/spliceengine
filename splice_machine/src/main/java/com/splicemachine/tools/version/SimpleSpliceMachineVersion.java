package com.splicemachine.tools.version;

import com.google.common.base.Splitter;

import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.lang.StringUtils.isBlank;

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
public class SimpleSpliceMachineVersion implements SpliceMachineVersion {

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

    SimpleSpliceMachineVersion(Map<String, String> manifestProps) {
        release = safeGet(manifestProps, "Release");
        implementationVersion = safeGet(manifestProps, "Implementation-Version");
        buildTime = safeGet(manifestProps, "Build-Time");
        url = safeGet(manifestProps, "URL");

        Iterator<String> versionParts = SPLITTER.split(release).iterator();
        majorVersion = versionParts.hasNext() ? safeParseInt(versionParts.next()) : UNKNOWN_INT;
        minorVersion = versionParts.hasNext() ? safeParseInt(versionParts.next()) : UNKNOWN_INT;
        patchVersion = versionParts.hasNext() ? safeParseInt(versionParts.next()) : UNKNOWN_INT;
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
            return matcher.find() ? Integer.valueOf(matcher.group()) : UNKNOWN_INT;
        } catch (NumberFormatException nfe) {
            return UNKNOWN_INT;
        }
    }
}
