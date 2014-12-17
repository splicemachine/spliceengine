package com.splicemachine.tools.version;

import org.junit.Test;

import java.util.jar.Attributes;
import java.util.jar.Manifest;

import static org.junit.Assert.assertEquals;

public class ManifestFinderTest {

    @Test
    public void findSpliceManifest() {
        ManifestFinder manifestFinder = new ManifestFinder("splice_machine_test", "com/splicemachine/tools/version/MANIFEST.MF");
        Manifest manifest = manifestFinder.findManifest();
        Attributes attributes = manifest.getMainAttributes();
        assertEquals("valueExpectedByManifestFinderTest", attributes.getValue("ManifestFinderTest"));
    }

}
