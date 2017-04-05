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
