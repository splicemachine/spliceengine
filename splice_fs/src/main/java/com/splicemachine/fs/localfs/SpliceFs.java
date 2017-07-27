/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
