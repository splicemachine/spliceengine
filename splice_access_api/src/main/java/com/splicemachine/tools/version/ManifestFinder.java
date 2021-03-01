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

import org.apache.log4j.Logger;
import splice.com.google.common.io.Closeables;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
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
    private final List<String> spliceJarFilePatterns;
    private final String manifestResourcePath;

    ManifestFinder() {
        this.spliceJarFilePatterns = Arrays.asList("/hbase_sql", "/mem_sql");
        this.manifestResourcePath = JarFile.MANIFEST_NAME;
    }

    ManifestFinder(String spliceJarFilePattern, String manifestResourcePath) {
        this.spliceJarFilePatterns = Arrays.asList(spliceJarFilePattern);
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
                for (String pattern : spliceJarFilePatterns) {
                    if (url.getFile().contains(pattern)) {
                        try (InputStream is = url.openStream()){
                            return is == null ? null : new Manifest(is);
                        }
                    }
                }
            }
        } catch (IOException e) {
            LOG.warn("Didn't find splice machine jar on the classpath. Version information will be unavailable.");
        }
        return null;
    }

}
