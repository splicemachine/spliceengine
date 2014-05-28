package com.splicemachine.derby.hbase;

import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Jeff Cunningham
 *         Date: 5/27/14
 */
public class ManifestReaderTest {

    @Test
    public void testReadManifestOnClasspath() throws Exception {
        boolean found = false;
        Enumeration resEnum;
        try {
            resEnum = Thread.currentThread().getContextClassLoader().getResources(JarFile.MANIFEST_NAME);

            while (resEnum.hasMoreElements()) {
                URL url = (URL) resEnum.nextElement();
                if (url.getFile().contains("splice") && url.getFile().contains("derbyclient")) {
                    found = true;
                    InputStream is = null;
                    try {
                        is = url.openStream();

                        if (is != null) {
                            Manifest manifest = new Manifest(is);

                            printManifest(manifest);
                        }
                    } finally {
                        if (is != null) {
                            is.close();
                        }
                    }
                }
            }
        } catch (Exception e) {
            // log
            throw e;
        }

        Assert.assertTrue("Didn't find splice derby client manifest", found);
    }

    @Test
    public void testSpliceMachineVersion() throws Exception {
        SpliceMachineVersion version = new TestManifestReader(new FakeManifest()).create();
        System.out.println(version);
        Assert.assertEquals("2014-05-27 12:52 -0500", version.getBuildTime());
        Assert.assertEquals("1d073ed3f6", version.getImplementationVersion());
        Assert.assertEquals("0.5.1-SNAPSHOT", version.getRelease());
        Assert.assertEquals("http://www.splicemachine.com", version.getURL());
    }

    private static void printManifest(Manifest manifest) throws Exception {
        System.out.println("====================================================");
        for (Map.Entry<Object,Object> entry : manifest.getMainAttributes().entrySet()) {
            System.out.println(entry.getKey()+" : "+entry.getValue());
        }
        System.out.println("====================================================");
    }

    public static class TestManifestReader extends ManifestReader {
        private final Manifest manifest;
        public TestManifestReader(Manifest manifest) {
            this.manifest = manifest;
        }

        @Override
        protected Map<String, String> readManifestOnClasspath() {
            return toMap(manifest);
        }
    }

    public static class FakeManifest extends Manifest {

        public FakeManifest() {
            super();
            super.getMainAttributes().putValue("Build-Time", "2014-05-27 12:52 -0500");
            super.getMainAttributes().putValue("Implementation-Version", "1d073ed3f6");
            super.getMainAttributes().putValue("Build-Jdk", "1.7.0_51");
            super.getMainAttributes().putValue("Built-By", "Jenkinson");
            super.getMainAttributes().putValue("Manifest-Version", "1.0");
            super.getMainAttributes().putValue("Created-By", "Apache Maven 3.1.1");
            super.getMainAttributes().putValue("Release", "0.5.1-SNAPSHOT");
            super.getMainAttributes().putValue("URL", "http://www.splicemachine.com");
            super.getMainAttributes().putValue("Archiver-Version", "Plexus Archiver");
        }
    }
}
