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
