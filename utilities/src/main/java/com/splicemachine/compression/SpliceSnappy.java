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

package com.splicemachine.compression;

import java.io.File;
import java.io.IOException;
import org.apache.log4j.Logger;
import org.xerial.snappy.OSInfo;
import org.xerial.snappy.Snappy;

public class SpliceSnappy {
    private static final Logger LOG = Logger.getLogger(SpliceSnappy.class);
    private static boolean installed = false;

    static {
        String tempDir = System.getProperty("org.xerial.snappy.tempdir");
        if (tempDir == null) {
            String userDir = System.getProperty("user.dir");
            if (userDir != null && new File(userDir).exists()) {
                System.setProperty("org.xerial.snappy.tempdir", userDir);
            }
        }

        // SPLICE-2115, workaround for snappy-java-1.0.4.1 on Mac
        if (OSInfo.getOSName().equals("Mac")) {
            System.setProperty("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");
        }

        try {
            Snappy.compress(new byte[16]);
            String version = Snappy.getNativeLibraryVersion();
            LOG.info("Snappy Installed (" + version + "): data will be compressed over the wire.");
            installed = true;
        }
        catch (Throwable t) {
            LOG.error("No Native Snappy Installed: data will not be compressed over the wire.", t);
        }
    }

    public static int maxCompressedLength(int byteSize) {
        return installed ? Snappy.maxCompressedLength(byteSize) : byteSize;
    }

    public static byte[] compress(byte[] bytes) throws IOException {
        if (installed) {
            bytes = Snappy.compress(bytes);
        }
        return bytes;
    }

    public static int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException {
        if (installed) {
            return Snappy.compress(input, inputOffset, inputLength, output, outputOffset);
        }
        System.arraycopy(input, inputOffset, output, outputOffset, inputLength);
        return inputLength;
    }

    public static byte[] uncompress(byte[] bytes) throws IOException {
        if (installed) {
            bytes = Snappy.uncompress(bytes);
        }
        return bytes;
    }

    public static int uncompress(byte[] bytes, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException {
        if (installed) {
            return Snappy.uncompress(bytes, inputOffset, inputLength, output, outputOffset);
        }
        return bytes.length;
    }

    public static int uncompressedLength(byte[] bytes, int inputOffset, int inputLength) throws IOException {
        if (installed) {
            return Snappy.uncompressedLength(bytes, inputOffset, inputLength);
        }
        return bytes.length;
    }
}
