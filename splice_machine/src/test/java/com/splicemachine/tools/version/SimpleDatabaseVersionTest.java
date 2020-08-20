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

import splice.com.google.common.collect.ImmutableMap;
import splice.com.google.common.collect.Maps;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

import static org.junit.Assert.*;

@Category(ArchitectureIndependent.class)
public class SimpleDatabaseVersionTest{

    @Test
    public void release() {
        Map<String, String> map = Maps.newHashMap();
        map.put("Build-Time", "2014-05-27");
        map.put("Implementation-Version", "1d073ed3f6");
        map.put("Release", "1.2.3");
        map.put("URL", "http://www.splicemachine.com");

        SimpleDatabaseVersion version = new SimpleDatabaseVersion(map);
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
        Map<String, String> map = ImmutableMap.of("Release", "2.3.4-SNAPSHOT");

        SimpleDatabaseVersion version = new SimpleDatabaseVersion(map);
        assertEquals("2.3.4-SNAPSHOT", version.getRelease());
        assertEquals(2, version.getMajorVersionNumber());
        assertEquals(3, version.getMinorVersionNumber());
        assertEquals(4, version.getPatchVersionNumber());
        assertFalse(version.isUnknown());
    }

    @Test
    public void releaseCandidate() {
        Map<String, String> map = ImmutableMap.of("Release", "4.5.55RC01");

        SimpleDatabaseVersion version = new SimpleDatabaseVersion(map);
        assertEquals("4.5.55RC01", version.getRelease());
        assertEquals(4, version.getMajorVersionNumber());
        assertEquals(5, version.getMinorVersionNumber());
        assertEquals(55, version.getPatchVersionNumber());
        assertFalse(version.isUnknown());
    }

    @Test
    public void releaseCandidateSnapshot() {
        Map<String, String> map = ImmutableMap.of("Release", "88.99.23004RC76-SNAPSHOT");

        SimpleDatabaseVersion version = new SimpleDatabaseVersion(map);
        assertEquals("88.99.23004RC76-SNAPSHOT", version.getRelease());
        assertEquals(88, version.getMajorVersionNumber());
        assertEquals(99, version.getMinorVersionNumber());
        assertEquals(23004, version.getPatchVersionNumber());
        assertFalse(version.isUnknown());
    }

    @Test
    public void fourDigitVersion() {
        Map<String, String> map = ImmutableMap.of("Release", "1.2.3.4");

        SimpleDatabaseVersion version = new SimpleDatabaseVersion(map);
        assertEquals("1.2.3.4", version.getRelease());
        assertEquals(1, version.getMajorVersionNumber());
        assertEquals(2, version.getMinorVersionNumber());
        assertEquals(3, version.getPatchVersionNumber());
        assertFalse(version.isUnknown());
    }

    @Test
    public void twoDigitVersion() {
        Map<String, String> map = ImmutableMap.of("Release", "1.2");

        SimpleDatabaseVersion version = new SimpleDatabaseVersion(map);
        assertEquals("1.2", version.getRelease());
        assertEquals(1, version.getMajorVersionNumber());
        assertEquals(2, version.getMinorVersionNumber());
        assertEquals(-1, version.getPatchVersionNumber());
        assertFalse(version.isUnknown());
    }

    @Test
    public void oneDigitVersion() {
        Map<String, String> map = ImmutableMap.of("Release", "1");

        SimpleDatabaseVersion version = new SimpleDatabaseVersion(map);
        assertEquals("1", version.getRelease());
        assertEquals(1, version.getMajorVersionNumber());
        assertEquals(-1, version.getMinorVersionNumber());
        assertEquals(-1, version.getPatchVersionNumber());
        assertFalse(version.isUnknown());
    }

    @Test
    public void emptyVersion() {
        Map<String, String> map = ImmutableMap.of("Release", "");

        SimpleDatabaseVersion version = new SimpleDatabaseVersion(map);
        assertEquals(SimpleDatabaseVersion.UNKNOWN_VERSION, version.getRelease());
        assertEquals(-1, version.getMajorVersionNumber());
        assertEquals(-1, version.getMinorVersionNumber());
        assertEquals(-1, version.getPatchVersionNumber());
        assertTrue(version.isUnknown());
    }

    @Test
    public void missingVersion() {
        Map<String, String> map = Maps.newTreeMap();

        SimpleDatabaseVersion version = new SimpleDatabaseVersion(map);
        assertEquals(SimpleDatabaseVersion.UNKNOWN_VERSION, version.getRelease());
        assertEquals(-1, version.getMajorVersionNumber());
        assertEquals(-1, version.getMinorVersionNumber());
        assertEquals(-1, version.getPatchVersionNumber());
        assertTrue(version.isUnknown());
    }

    @Test
    public void nullVersion() {
        SimpleDatabaseVersion version = new SimpleDatabaseVersion(null);
        assertEquals(SimpleDatabaseVersion.UNKNOWN_VERSION, version.getRelease());
        assertEquals(-1, version.getMajorVersionNumber());
        assertEquals(-1, version.getMinorVersionNumber());
        assertEquals(-1, version.getPatchVersionNumber());
        assertTrue(version.isUnknown());
    }

    @Test
    public void crazyVersion() {
        Map<String, String> map = ImmutableMap.of("Release", "a1a.b2b.c3c");

        SimpleDatabaseVersion version = new SimpleDatabaseVersion(map);
        assertEquals("a1a.b2b.c3c", version.getRelease());
        assertEquals(1, version.getMajorVersionNumber());
        assertEquals(2, version.getMinorVersionNumber());
        assertEquals(3, version.getPatchVersionNumber());
        assertFalse(version.isUnknown());
    }

}
