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

package com.splicemachine.tools.version;

import org.sparkproject.guava.collect.ImmutableMap;
import org.sparkproject.guava.collect.Maps;
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