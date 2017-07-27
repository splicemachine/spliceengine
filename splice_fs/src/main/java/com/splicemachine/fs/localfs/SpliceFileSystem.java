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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.security.AccessControlException;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Random;

/****************************************************************
 * Implement the FileSystem API for the checksumed local filesystem.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SpliceFileSystem extends ChecksumFileSystem {
    private static Logger LOG=Logger.getLogger(SpliceFileSystem.class);

    public static final URI NAME = URI.create("splice:///");
    public static final String SCHEME = "splice";
    static private Random rand = new Random();

    public SpliceFileSystem() {
        this(new RawLocalFileSystem() {
            @Override
            public URI getUri() {
                return NAME;
            }

            @Override
            public FileStatus getFileStatus(Path p) throws IOException {
                return new SpliceFileStatus(super.getFileStatus(p));
            }
        });
    }

    @Override
    public URI getUri() {
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("getURI name=%s",NAME));
        return NAME;
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("initialize name=%s",name));
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
        return SCHEME;
    }

    public FileSystem getRaw() {
        return getRawFileSystem();
    }

    public SpliceFileSystem(FileSystem rawLocalFileSystem) {
        super(rawLocalFileSystem);
    }

    /** Convert a path to a File. */
    public File pathToFile(Path path) {
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("pathToFile path=%s",path));
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
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("getFileLinkStatus path=%s",f));
        return fs.getFileLinkStatus(f);
    }

    @Override
    public Path getLinkTarget(Path f) throws IOException {
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("getLinkTarget path=%s",f));
        return fs.getLinkTarget(f);
    }

    /**
     *
     * Modified to support Amazon EFS
     *
     * @param p
     * @return
     * @throws IOException
     */
    @Override
    public FsStatus getStatus(Path p) throws IOException {
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("getStatus path=%s",p));
        File partition = pathToFile(p == null ? new Path("/") : p);
        //File provides getUsableSpace() and getFreeSpace()
        //File provides no API to obtain used space, assume used = total - free
        return new FsStatus(Long.MAX_VALUE,
                125l,
                Long.MAX_VALUE-125l);
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        return new SpliceFileStatus(super.getFileStatus(f));
    }

    @Override
    public void access(Path path, FsAction mode) throws AccessControlException,
            FileNotFoundException, IOException {
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("access path=%s, mode=%s",path,mode));
        fs.access(new Path(path.toUri().getRawPath()),mode);
    }



}
