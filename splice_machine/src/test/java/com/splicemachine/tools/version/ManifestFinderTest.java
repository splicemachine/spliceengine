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

import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.jar.Attributes;
import java.util.jar.Manifest;

import static org.junit.Assert.assertEquals;

@Category(ArchitectureIndependent.class)
@Ignore("-sf- ignore until classpath issues can be resolved")
public class ManifestFinderTest {

    @Test
    public void findSpliceManifest() {
        ManifestFinder manifestFinder = new ManifestFinder("splice_machine_test","/com/splicemachine/tools/version/MANIFEST.MF");
        Manifest manifest = manifestFinder.findManifest();
        Attributes attributes = manifest.getMainAttributes();
        assertEquals("valueExpectedByManifestFinderTest", attributes.getValue("ManifestFinderTest"));
    }

}
