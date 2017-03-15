package com.splicemachine.fs.s3;

import org.apache.hadoop.fs.FileStatus;

/**
 * Created by jleach on 3/14/17.
 */
public final class HadoopFileStatus {
    private HadoopFileStatus() {
    }

    public static boolean isDirectory(FileStatus status) {
        return status.isDirectory();
    }

    public static boolean isFile(FileStatus status) {
        return status.isFile();
    }
}
