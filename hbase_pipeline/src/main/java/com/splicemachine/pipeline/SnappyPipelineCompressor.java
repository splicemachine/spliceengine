/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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

package com.splicemachine.pipeline;

import java.io.IOException;
import org.apache.log4j.Logger;
import org.xerial.snappy.Snappy;

import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class SnappyPipelineCompressor implements PipelineCompressor{
    private static final Logger LOG=Logger.getLogger(SnappyPipelineCompressor.class);
    private static final boolean supportsNative;

    static {
        boolean installed = false;
        try {
            String version = Snappy.getNativeLibraryVersion();
            SpliceLogUtils.info(LOG, "Snappy Installed (" + version + "): Splice Machine's Write Pipeline will compress data over the wire.");
            installed = true;
        }
        catch (Exception ex) {
            SpliceLogUtils.error(LOG,"No Native Snappy Installed: Splice Machine's Write Pipeline will not compress data over the wire.");
        }
        supportsNative = installed;
    }

    private final PipelineCompressor delegate;

    public SnappyPipelineCompressor(PipelineCompressor delegate){
        this.delegate=delegate;
    }

    @Override
    public byte[] compress(Object o) throws IOException {
        byte[] d = delegate.compress(o);
        if (supportsNative) {
            d = Snappy.compress(d);
        }
        return d;
    }

    @Override
    public <T> T decompress(byte[] bytes,Class<T> clazz) throws IOException {
        byte[] d = bytes;
        if (supportsNative) {
            d = Snappy.uncompress(bytes);
        }
        return delegate.decompress(d, clazz);
    }
}
