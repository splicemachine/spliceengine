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
package com.splicemachine.io.hbase.encoding;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

/**
 *
 * Dummy File System for the orc Writer.  Pass through to the DataOutputStream.
 *
 */
public class PAXBlockFileSystem extends FileSystem {
    private DataOutputStream out;
    public PAXBlockFileSystem(DataOutputStream out) {
        this.out = out;
    }

    /**
     *
     * Unsupported Operations
     *
     * @return
     */
    @Override
    public URI getUri() {
        throw new UnsupportedOperationException("Not Supported");
    }

    /**
     *
     * Unsupported Operations
     *
     * @param f
     * @param bufferSize
     * @return
     * @throws IOException
     */
    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    /**
     *
     * FSDataOutputStream wrapping an output stream.
     *
     * @param f
     * @param permission
     * @param overwrite
     * @param bufferSize
     * @param replication
     * @param blockSize
     * @param progress
     * @return
     * @throws IOException
     */
    @Override
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
        return new FSDataOutputStream(out,null);
    }

    /**
     *
     * Unsupported Operation
     *
     * @param f
     * @param bufferSize
     * @param progress
     * @return
     * @throws IOException
     */
    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    /**
     *
     *
     * Unsupported Operation
     *
     * @param src
     * @param dst
     * @return
     * @throws IOException
     */
    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }
    /**
     *
     *
     * Unsupported Operation
     *
     * @param src
     * @param dst
     * @return
     * @throws IOException
     */
    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    /**
     *
     * Unsupported Operation
     *
     * @param f
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    /**
     *
     * Unsupported Operation
     *
     * @param new_dir
     */
    @Override
    public void setWorkingDirectory(Path new_dir) {
        throw new UnsupportedOperationException("Not Supported");
    }

    /**
     *
     * Unsupported Operation
     *
     * @return
     */
    @Override
    public Path getWorkingDirectory() {
        throw new UnsupportedOperationException("Not Supported");
    }

    /**
     *
     * Unsupported Operation
     *
     * @param f
     * @param permission
     * @return
     * @throws IOException
     */
    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    /**
     *
     * Unsupported Operation
     *
     * @param f
     * @return
     * @throws IOException
     */
    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }
}