/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
