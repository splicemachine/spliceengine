package org.apache.hadoop.fs;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.StringTokenizer;

/**
 *
 * Wrapper around FileStatus to attempt Native Posix File Permissions.
 *
 */
public class SpliceFileStatus extends FileStatus {
    private static Logger LOG=Logger.getLogger(SpliceFileStatus.class);

    FileStatus fileStatus;

    public SpliceFileStatus(FileStatus fileStatus) {
        this.fileStatus = fileStatus;
    }

    @Override
    public long getLen() {
        return fileStatus.getLen();
    }

    @Override
    public boolean isFile() {
        return fileStatus.isFile();
    }

    @Override
    public boolean isDirectory() {
        return fileStatus.isDirectory();
    }

    @Override
    public boolean isSymlink() {
        return fileStatus.isSymlink();
    }

    @Override
    public long getBlockSize() {
        return fileStatus.getBlockSize();
    }

    @Override
    public short getReplication() {
        return fileStatus.getReplication();
    }

    @Override
    public long getModificationTime() {
        return fileStatus.getModificationTime();
    }

    @Override
    public long getAccessTime() {
        return fileStatus.getAccessTime();
    }

    @Override
    public boolean isEncrypted() {
        return fileStatus.isEncrypted();
    }

    @Override
    public Path getPath() {
        return fileStatus.getPath();
    }

    @Override
    public void setPath(Path p) {
        fileStatus.setPath(p);
    }

    @Override
    public void setPermission(FsPermission permission) {
        fileStatus.setPermission(permission);
    }

    @Override
    public void setOwner(String owner) {
        fileStatus.setOwner(owner);
    }

    @Override
    public void setGroup(String group) {
        fileStatus.setGroup(group);
    }

    @Override
    public Path getSymlink() throws IOException {
        return fileStatus.getSymlink();
    }

    @Override
    public void setSymlink(Path p) {
        fileStatus.setSymlink(p);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        fileStatus.readFields(in);
    }

    @Override
    public int compareTo(Object o) {
        return fileStatus.compareTo(o);
    }

    @Override
    public boolean equals(Object o) {
        return fileStatus.equals(o);
    }

    @Override
    public int hashCode() {
        return fileStatus.hashCode();
    }

    @Override
    public String toString() {
        return fileStatus.toString();
    }



    private boolean isPermissionLoaded() {
        return !super.getOwner().isEmpty();
    }

    @Override
    public FsPermission getPermission() {
        if (!isPermissionLoaded()) {
            loadPermissionInfo();
        }
        return super.getPermission();
    }

    @Override
    public String getOwner() {
        if (!isPermissionLoaded()) {
            loadPermissionInfo();
        }
        return super.getOwner();
    }

    @Override
    public String getGroup() {
        if (!isPermissionLoaded()) {
            loadPermissionInfo();
        }
        return super.getGroup();
    }

    /// loads permissions, owner, and group from `ls -ld`
    private void loadPermissionInfo() {

        if (NativeIO.isAvailable()) {
            FileInputStream fis = null;
            try {
                File f = new File(getPath().toUri().getRawPath());
                FileDescriptor fd = NativeIO.POSIX.open(f.getAbsolutePath(),
                        NativeIO.POSIX.O_RDONLY, 0);
                fis = new FileInputStream(fd);
                NativeIO.POSIX.Stat stat = NativeIO.POSIX.getFstat(fis.getFD());
                setPermission(new FsPermission((short)stat.getMode()));
                setOwner(stat.getOwner());
                setGroup(stat.getGroup());
                return;
            } catch (Throwable e) {
                // ignore
                LOG.warn("NativeIO Exception", e);
            } finally {
                if (fis != null) {
                    try {
                        fis.close();
                    } catch (Throwable e) {
                        // ignore
                    }
                }
             }
         }


        IOException e = null;
        try {
            String output = FileUtil.execCommand(new File(getPath().toUri().getRawPath()),
                    Shell.getGetPermissionCommand());
            StringTokenizer t =
                    new StringTokenizer(output, Shell.TOKEN_SEPARATOR_REGEX);
            //expected format
            //-rw-------    1 username groupname ...
            String permission = t.nextToken();
            if (permission.length() > FsPermission.MAX_PERMISSION_LENGTH) {
                //files with ACLs might have a '+'
                permission = permission.substring(0,
                        FsPermission.MAX_PERMISSION_LENGTH);
            }
            setPermission(FsPermission.valueOf(permission));
            t.nextToken();

            String owner = t.nextToken();
            // If on windows domain, token format is DOMAIN\\user and we want to
            // extract only the user name
            if (Shell.WINDOWS) {
                int i = owner.indexOf('\\');
                if (i != -1)
                    owner = owner.substring(i + 1);
            }
            setOwner(owner);

            setGroup(t.nextToken());
        } catch (Shell.ExitCodeException ioe) {
            if (ioe.getExitCode() != 1) {
                e = ioe;
            } else {
                setPermission(null);
                setOwner(null);
                setGroup(null);
            }
        } catch (IOException ioe) {
            e = ioe;
        } finally {
            if (e != null) {
                throw new RuntimeException("Error while running command to get " +
                        "file permissions : " +
                        StringUtils.stringifyException(e));
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }



}
