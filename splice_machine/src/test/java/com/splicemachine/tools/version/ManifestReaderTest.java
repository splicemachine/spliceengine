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
import com.splicemachine.si.testenv.ArchitectureIndependent;
import com.splicemachine.si.testenv.ArchitectureSpecific;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.jar.Attributes;
import java.util.jar.Manifest;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(ArchitectureIndependent.class)
public class ManifestReaderTest {

    @Test
    public void createVersion_snapshot() throws Exception {
        ManifestFinder manifestFinder = mockManifest("0.5.1-SNAPSHOT");
        ManifestReader manifestReader = new ManifestReader(manifestFinder);
        DatabaseVersion version = manifestReader.createVersion();

        assertEquals("2014-05-27 12:52 -0500", version.getBuildTime());
        assertEquals("1d073ed3f6", version.getImplementationVersion());
        assertEquals("http://www.splicemachine.com", version.getURL());
        assertEquals("0.5.1-SNAPSHOT", version.getRelease());
        assertEquals(0, version.getMajorVersionNumber());
        assertEquals(5, version.getMinorVersionNumber());
        assertEquals(1, version.getPatchVersionNumber());
    }

    @Test
    public void createVersion_releaseCandidate() throws Exception {
        ManifestFinder manifestFinder = mockManifest("4.3.2RC16");
        ManifestReader manifestReader = new ManifestReader(manifestFinder);
        DatabaseVersion version = manifestReader.createVersion();

        assertEquals("2014-05-27 12:52 -0500", version.getBuildTime());
        assertEquals("1d073ed3f6", version.getImplementationVersion());
        assertEquals("http://www.splicemachine.com", version.getURL());
        assertEquals("4.3.2RC16", version.getRelease());
        assertEquals(4, version.getMajorVersionNumber());
        assertEquals(3, version.getMinorVersionNumber());
        assertEquals(2, version.getPatchVersionNumber());
    }

    @Test
    public void createVersion_releaseCandidate_snapshot() throws Exception {
        ManifestFinder manifestFinder = mockManifest("88.77.66RC16-SNAPSHOT");
        ManifestReader manifestReader = new ManifestReader(manifestFinder);
        DatabaseVersion version = manifestReader.createVersion();

        assertEquals("2014-05-27 12:52 -0500", version.getBuildTime());
        assertEquals("1d073ed3f6", version.getImplementationVersion());
        assertEquals("http://www.splicemachine.com", version.getURL());
        assertEquals("88.77.66RC16-SNAPSHOT", version.getRelease());
        assertEquals(88, version.getMajorVersionNumber());
        assertEquals(77, version.getMinorVersionNumber());
        assertEquals(66, version.getPatchVersionNumber());
    }

    @Test
    public void createVersion_release() throws Exception {
        ManifestFinder manifestFinder = mockManifest("1.0.3");
        ManifestReader manifestReader = new ManifestReader(manifestFinder);
        DatabaseVersion version = manifestReader.createVersion();

        assertEquals("2014-05-27 12:52 -0500", version.getBuildTime());
        assertEquals("1d073ed3f6", version.getImplementationVersion());
        assertEquals("http://www.splicemachine.com", version.getURL());
        assertEquals("1.0.3", version.getRelease());
        assertEquals(1, version.getMajorVersionNumber());
        assertEquals(0, version.getMinorVersionNumber());
        assertEquals(3, version.getPatchVersionNumber());
    }

    public static ManifestFinder mockManifest(String releaseVersion) {

        Attributes attributes = new Attributes();
        attributes.putValue("Build-Time", "2014-05-27 12:52 -0500");
        attributes.putValue("Implementation-Version", "1d073ed3f6");
        attributes.putValue("Build-Jdk", "1.7.0_51");
        attributes.putValue("Built-By", "Jenkinson");
        attributes.putValue("Manifest-Version", "1.0");
        attributes.putValue("Created-By", "Apache Maven 3.1.1");
        attributes.putValue("Release", releaseVersion);
        attributes.putValue("URL", "http://www.splicemachine.com");
        attributes.putValue("Archiver-Version", "Plexus Archiver");

        Manifest spliceManifest = mock(Manifest.class);
        when(spliceManifest.getMainAttributes()).thenReturn(attributes);

        ManifestFinder mockFinder = mock(ManifestFinder.class);
        when(mockFinder.findManifest()).thenReturn(spliceManifest);

        return mockFinder;
    }

}
