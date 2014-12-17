package com.splicemachine.tools.version;

import com.google.common.io.Closeables;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Returns an object representing the META-INF/MANIFEST.MF from a specified jar/file.
 *
 * This should be rewritten to not depend on the jar name/path.  Instead look at all classpath manifests and
 * return the one that contains keys/values that indicate that it is the one we are interested in.
 */
class ManifestFinder {

    private static final Logger LOG = Logger.getLogger(ManifestFinder.class);

    /* Find it in a path/file that contains this string. */
    private final String spliceJarFilePattern;

    ManifestFinder() {
        this.spliceJarFilePattern = "/splice_machine";
    }

    ManifestFinder(String spliceJarFilePattern) {
        this.spliceJarFilePattern = spliceJarFilePattern;
    }

    /**
     * Read the manifest from the splice_machine-*.jar
     *
     * @return manifest entries mapped to strings.
     */
    public Manifest findManifest() {
        try {
            Enumeration resEnum = Thread.currentThread().getContextClassLoader().getResources(JarFile.MANIFEST_NAME);
            while (resEnum.hasMoreElements()) {
                URL url = (URL) resEnum.nextElement();
                if (url.getFile().contains(this.spliceJarFilePattern)) {
                    InputStream is = null;
                    try {
                        is = url.openStream();
                        return is == null ? null : new Manifest(is);
                    } finally {
                        Closeables.closeQuietly(is);
                    }
                }
            }
        } catch (IOException e) {
            SpliceLogUtils.warn(LOG, "Didn't find splice machine jar on the classpath. Version information will be unavailable.");
        }
        return null;
    }

}
