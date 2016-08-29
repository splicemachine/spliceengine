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

import org.apache.log4j.Logger;
import org.spark_project.guava.io.Closeables;
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
    private final String manifestResourcePath;

    ManifestFinder() {
        this.spliceJarFilePattern = "/splice_machine";
        this.manifestResourcePath = JarFile.MANIFEST_NAME;
    }

    ManifestFinder(String spliceJarFilePattern, String manifestResourcePath) {
        this.spliceJarFilePattern = spliceJarFilePattern;
        this.manifestResourcePath = manifestResourcePath;
    }

    /**
     * Read the manifest from the splice_machine-*.jar
     *
     * @return manifest entries mapped to strings.
     */
    public Manifest findManifest() {
        try {
            Enumeration resEnum = Thread.currentThread().getContextClassLoader().getResources(this.manifestResourcePath);
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
            LOG.warn("Didn't find splice machine jar on the classpath. Version information will be unavailable.");
        }
        return null;
    }

}
