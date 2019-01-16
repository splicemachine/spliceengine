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
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.local.LocalConfigKeys;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 * RawSpliceFs provided to wrap SpliceFileSystem for Yarn Implementaton.
 *
 */
public class RawSpliceFs extends DelegateToFileSystem {
    private static Logger LOG=Logger.getLogger(RawSpliceFs.class);

    RawSpliceFs(final Configuration conf) throws IOException, URISyntaxException {
        this(SpliceFileSystem.NAME, conf);
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
    RawSpliceFs(final URI theUri, final Configuration conf) throws IOException,
            URISyntaxException {
        super(theUri, new SpliceFileSystem(), conf,
                SpliceFileSystem.SCHEME, false);
    }

    @Override
    public int getUriDefaultPort() {
        return -1; // No default port for file:///
    }

    @Override
    public FsServerDefaults getServerDefaults() throws IOException {
        return LocalConfigKeys.getServerDefaults();
    }

    @Override
    public boolean isValidName(String src) {
        // Different local file systems have different validation rules. Skip
        // validation here and just let the OS handle it. This is consistent with
        // RawLocalFileSystem.
        return true;
    }


    @Override
    public URI getUri() {
        return SpliceFileSystem.NAME;
    }
}

