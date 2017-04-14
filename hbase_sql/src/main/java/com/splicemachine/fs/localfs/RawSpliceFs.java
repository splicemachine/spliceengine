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
