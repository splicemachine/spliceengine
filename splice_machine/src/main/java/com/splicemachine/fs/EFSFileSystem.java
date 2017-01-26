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

package com.splicemachine.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Random;

/****************************************************************
 * Implement the FileSystem API for the checksumed local filesystem.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class EFSFileSystem extends ChecksumFileSystem {
    static final URI NAME = URI.create("efs:///");
    static private Random rand = new Random();

    public EFSFileSystem() {
        this(new RawLocalFileSystem());
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        if (fs.getConf() == null) {
            fs.initialize(name, conf);
        }
        String scheme = name.getScheme();
        if (!scheme.equals(fs.getUri().getScheme())) {
            swapScheme = scheme;
        }
    }

    /**
     * Return the protocol scheme for the FileSystem.
     * <p/>
     *
     * @return <code>file</code>
     */
    @Override
    public String getScheme() {
        return "efs";
    }

    public FileSystem getRaw() {
        return getRawFileSystem();
    }

    public EFSFileSystem(FileSystem rawLocalFileSystem) {
        super(rawLocalFileSystem);
    }

    /** Convert a path to a File. */
    public File pathToFile(Path path) {
        return ((RawLocalFileSystem)fs).pathToFile(path);
    }

    @Override
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException {
        FileUtil.copy(this, src, this, dst, delSrc, getConf());
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst)
            throws IOException {
        FileUtil.copy(this, src, this, dst, delSrc, getConf());
    }

    /**
     * Moves files to a bad file directory on the same device, so that their
     * storage will not be reused.
     */
    @Override
    public boolean reportChecksumFailure(Path p, FSDataInputStream in,
                                         long inPos,
                                         FSDataInputStream sums, long sumsPos) {
        try {
            // canonicalize f
            File f = ((RawLocalFileSystem)fs).pathToFile(p).getCanonicalFile();

            // find highest writable parent dir of f on the same device
            String device = new DF(f, getConf()).getMount();
            File parent = f.getParentFile();
            File dir = null;
            while (parent != null && FileUtil.canWrite(parent) &&
                    parent.toString().startsWith(device)) {
                dir = parent;
                parent = parent.getParentFile();
            }

            if (dir==null) {
                throw new IOException(
                        "not able to find the highest writable parent dir");
            }

            // move the file there
            File badDir = new File(dir, "bad_files");
            if (!badDir.mkdirs()) {
                if (!badDir.isDirectory()) {
                    throw new IOException("Mkdirs failed to create " + badDir.toString());
                }
            }
            String suffix = "." + rand.nextInt();
            File badFile = new File(badDir, f.getName()+suffix);
            LOG.warn("Moving bad file " + f + " to " + badFile);
            in.close();                               // close it first
            boolean b = f.renameTo(badFile);                      // rename it
            if (!b) {
                LOG.warn("Ignoring failure of renameTo");
            }
            // move checksum file too
            File checkFile = ((RawLocalFileSystem)fs).pathToFile(getChecksumFile(p));
            // close the stream before rename to release the file handle
            sums.close();
            b = checkFile.renameTo(new File(badDir, checkFile.getName()+suffix));
            if (!b) {
                LOG.warn("Ignoring failure of renameTo");
            }
        } catch (IOException e) {
            LOG.warn("Error moving bad file " + p + ": " + e);
        }
        return false;
    }

    @Override
    public boolean supportsSymlinks() {
        return true;
    }

    @Override
    public void createSymlink(Path target, Path link, boolean createParent)
            throws IOException {
        fs.createSymlink(target, link, createParent);
    }

    @Override
    public FileStatus getFileLinkStatus(final Path f) throws IOException {
        return fs.getFileLinkStatus(f);
    }

    @Override
    public Path getLinkTarget(Path f) throws IOException {
        return fs.getLinkTarget(f);
    }

    @Override
    public FsStatus getStatus(Path p) throws IOException {
        File partition = pathToFile(p == null ? new Path("/") : p);
        //File provides getUsableSpace() and getFreeSpace()
        //File provides no API to obtain used space, assume used = total - free
        return new FsStatus(Long.MAX_VALUE,
                125l,
                Long.MAX_VALUE-125l);
    }




}
