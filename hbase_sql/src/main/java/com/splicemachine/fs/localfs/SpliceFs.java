/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
package com.splicemachine.fs.localfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.ChecksumFs;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 * Inherits from the ChecksumFs and creates the RawSpliceFs underneath.  This
 * implementation is required for Yarn.
 *
 */
public class SpliceFs extends ChecksumFs {
    private static Logger LOG=Logger.getLogger(SpliceFs.class);

    SpliceFs(final Configuration conf) throws IOException, URISyntaxException {
            super(new RawSpliceFs(conf));
        }

    @Override
    public URI getUri() {
        return SpliceFileSystem.NAME;
    }

    /**
         * This constructor has the signature needed by
         * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}.
         *
         * @param theUri which must be that of localFs
         * @param conf
         * @throws IOException
         * @throws URISyntaxException
         */
        SpliceFs(final URI theUri, final Configuration conf) throws IOException,
                URISyntaxException {
            this(conf);
        }

    @Override
    public void checkPath(Path path) {
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("checkPath %s",path));
        super.checkPath(path);
    }
}

