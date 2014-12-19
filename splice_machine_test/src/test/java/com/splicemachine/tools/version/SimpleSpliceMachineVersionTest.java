package com.splicemachine.tools.version;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class SimpleSpliceMachineVersionTest {

    @Test
    public void release() {
        Map<String, String> map = Maps.newHashMap();
        map.put("Build-Time", "2014-05-27");
        map.put("Implementation-Version", "1d073ed3f6");
        map.put("Release", "1.2.3");
        map.put("URL", "http://www.splicemachine.com");

        SimpleSpliceMachineVersion version = new SimpleSpliceMachineVersion(map);
        assertEquals("2014-05-27", version.getBuildTime());
        assertEquals("1d073ed3f6", version.getImplementationVersion());
        assertEquals("1.2.3", version.getRelease());
        assertEquals("http://www.splicemachine.com", version.getURL());
        assertEquals(1, version.getMajorVersionNumber());
        assertEquals(2, version.getMinorVersionNumber());
        assertEquals(3, version.getPatchVersionNumber());
        assertFalse(version.isUnknown());
    }

    @Test
    public void snapshot() {
        Map<String, String> map = Maps.newHashMap();
        map.put("Release", "2.3.4-SNAPSHOT");

        SimpleSpliceMachineVersion version = new SimpleSpliceMachineVersion(map);
        assertEquals("2.3.4-SNAPSHOT", version.getRelease());
        assertEquals(2, version.getMajorVersionNumber());
        assertEquals(3, version.getMinorVersionNumber());
        assertEquals(4, version.getPatchVersionNumber());
        assertFalse(version.isUnknown());
    }

    @Test
    public void releaseCandidate() {
        Map<String, String> map = Maps.newHashMap();
        map.put("Release", "4.5.55RC01");

        SimpleSpliceMachineVersion version = new SimpleSpliceMachineVersion(map);
        assertEquals("4.5.55RC01", version.getRelease());
        assertEquals(4, version.getMajorVersionNumber());
        assertEquals(5, version.getMinorVersionNumber());
        assertEquals(55, version.getPatchVersionNumber());
        assertFalse(version.isUnknown());
    }

    @Test
    public void releaseCandidateSnapshot() {
        Map<String, String> map = Maps.newHashMap();
        map.put("Release", "88.99.23004RC76-SNAPSHOT");

        SimpleSpliceMachineVersion version = new SimpleSpliceMachineVersion(map);
        assertEquals("88.99.23004RC76-SNAPSHOT", version.getRelease());
        assertEquals(88, version.getMajorVersionNumber());
        assertEquals(99, version.getMinorVersionNumber());
        assertEquals(23004, version.getPatchVersionNumber());
        assertFalse(version.isUnknown());
    }

    @Test
    public void fourDigitVersion() {
        Map<String, String> map = Maps.newHashMap();
        map.put("Release", "1.2.3.4");

        SimpleSpliceMachineVersion version = new SimpleSpliceMachineVersion(map);
        assertEquals("1.2.3.4", version.getRelease());
        assertEquals(1, version.getMajorVersionNumber());
        assertEquals(2, version.getMinorVersionNumber());
        assertEquals(3, version.getPatchVersionNumber());
        assertFalse(version.isUnknown());
    }

    @Test
    public void twoDigitVersion() {
        Map<String, String> map = ImmutableMap.of("Release", "1.2");

        SimpleSpliceMachineVersion version = new SimpleSpliceMachineVersion(map);
        assertEquals("1.2", version.getRelease());
        assertEquals(1, version.getMajorVersionNumber());
        assertEquals(2, version.getMinorVersionNumber());
        assertEquals(-1, version.getPatchVersionNumber());
        assertFalse(version.isUnknown());
    }

    @Test
    public void oneDigitVersion() {
        Map<String, String> map = ImmutableMap.of("Release", "1");

        SimpleSpliceMachineVersion version = new SimpleSpliceMachineVersion(map);
        assertEquals("1", version.getRelease());
        assertEquals(1, version.getMajorVersionNumber());
        assertEquals(-1, version.getMinorVersionNumber());
        assertEquals(-1, version.getPatchVersionNumber());
        assertFalse(version.isUnknown());
    }

    @Test
    public void emptyVersion() {
        Map<String, String> map = ImmutableMap.of("Release", "");

        SimpleSpliceMachineVersion version = new SimpleSpliceMachineVersion(map);
        assertEquals(SimpleSpliceMachineVersion.UNKNOWN_VERSION, version.getRelease());
        assertEquals(-1, version.getMajorVersionNumber());
        assertEquals(-1, version.getMinorVersionNumber());
        assertEquals(-1, version.getPatchVersionNumber());
        assertTrue(version.isUnknown());
    }

    @Test
    public void missingVersion() {
        Map<String, String> map = Maps.newTreeMap();

        SimpleSpliceMachineVersion version = new SimpleSpliceMachineVersion(map);
        assertEquals(SimpleSpliceMachineVersion.UNKNOWN_VERSION, version.getRelease());
        assertEquals(-1, version.getMajorVersionNumber());
        assertEquals(-1, version.getMinorVersionNumber());
        assertEquals(-1, version.getPatchVersionNumber());
        assertTrue(version.isUnknown());
    }

    @Test
    public void nullVersion() {
        SimpleSpliceMachineVersion version = new SimpleSpliceMachineVersion(null);
        assertEquals(SimpleSpliceMachineVersion.UNKNOWN_VERSION, version.getRelease());
        assertEquals(-1, version.getMajorVersionNumber());
        assertEquals(-1, version.getMinorVersionNumber());
        assertEquals(-1, version.getPatchVersionNumber());
        assertTrue(version.isUnknown());
    }

    @Test
    public void crazyVersion() {
        Map<String, String> map = ImmutableMap.of("Release", "a1a.b2b.c3c");

        SimpleSpliceMachineVersion version = new SimpleSpliceMachineVersion(map);
        assertEquals("a1a.b2b.c3c", version.getRelease());
        assertEquals(1, version.getMajorVersionNumber());
        assertEquals(2, version.getMinorVersionNumber());
        assertEquals(3, version.getPatchVersionNumber());
        assertFalse(version.isUnknown());
    }

}