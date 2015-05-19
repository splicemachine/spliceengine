package com.splicemachine.hbase.backup;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.FSDataInputStreamWrapper;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.HalfStoreFileReader;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.log4j.Logger;

import java.util.*;

import java.io.IOException;

/**
 * Created by jyuan on 5/14/15.
 */
public abstract class SnapshotUtilsBase implements SnapshotUtils {

    static final Logger LOG = Logger.getLogger(SnapshotUtilsBase.class);
    /**
     * Materializes snapshot reference file - creates real hfile
     * in a current region's tmp directory.
     *
     */
    @Override
    public Path materializeRefFile(Configuration conf,
                                   FileSystem fs,
                                   HFileLink refFilePath,
                                   HRegion region) throws IOException {

        Path localDir = new Path(BackupUtils.derbyFactory.getRegionDir(region) + "/.tmp");
        Path outFile = BackupUtils.getRandomFilename(fs, localDir);
        Reference reference = readReference(fs, refFilePath);
        HFileLink hfile = getReferredFileLink(refFilePath);
        HColumnDescriptor family = region.getTableDesc().getFamily(SpliceConstants.DEFAULT_FAMILY_BYTES);
        BackupUtils.copyHFileHalf(conf, hfile.getAvailablePath(fs), outFile, reference, family);

        return  outFile;
    }


    /**
     * Returns column family name from store file path
     * @param link
     * @return column family name (as byte array)
     */
    public byte[] getColumnFamily(HFileLink link)
    {
        Path path = link.getOriginPath();
        return path.getParent().getName().getBytes();
    }

    private Reference readReference(FileSystem fs, HFileLink link)
            throws IOException
    {
        int totalAttempts = 0;
        int maxAttempts = link.getLocations().length;
        while( totalAttempts ++ <= maxAttempts){
            try{
                Path p = link.getAvailablePath(fs);
                return Reference.read(fs, p);
            } catch (Exception e){
                if(totalAttempts == maxAttempts) {
                    throw e;
                }
            }
        }
        // should not be here
        return null;
    }

    /**
     * Returns path to a parent store file for a given reference file
     *
     * Example:
     * Input:
     * /TABLE_A/a60772afe8c4aa3355360d3a6de0b292/fam_a/9fb67500d79a43e79b01da8d5d3017a4.88a177637e155be4d01f21441bf8595d
     * Output:
     * /TABLE_A/88a177637e155be4d01f21441bf8595d/fam_a/9fb67500d79a43e79b01da8d5d3017a4
     * @param refFilePath
     * @return parent store file path
     */
    public Path getReferredFile(Path refFilePath)
    {
        String[] parts = refFilePath.getName().split("\\.");
        // parts[0] - store file name
        // parts[1] - encoded region name
        Path p = refFilePath.getParent();
        String columnFamily = p.getName();
        p = p.getParent().getParent();
        // Add region
        p = new Path(p, parts[1]);
        // Add columnFamily
        p = new Path(p, columnFamily);
        // Add store file name
        p = new Path(p, parts[0]);
        return p;
    }

    protected abstract HFileLink getReferredFileLink(HFileLink ref) throws IOException;

    protected String getTableName(Path refFilePath) {
        Path p = refFilePath.getParent().getParent().getParent();
        return p.getName();
    }

    protected String getColumnFamilyName(Path refFilePath) {
        Path p = refFilePath.getParent();
        return p.getName();
    }

    protected String getRegionName(Path refFilePath) {
        String[] parts = refFilePath.getName().split("\\.");
        return parts[1];
    }

    protected String getFileName(Path refFilePath) {
        String[] parts = refFilePath.getName().split("\\.");
        return parts[0];
    }

    protected boolean isRegionTheSame(String fullName, String shortId)
    {
        return fullName.indexOf(shortId) >=0;
    }

    /**
     * Checks if region info for the current region.
     * Essentially, it just a string compare
     * @param region
     * @param regInfo
     * @return true if yes
     */
    protected boolean isCurrentRegion(HRegion region, HRegionInfo regInfo) {
        return region.getRegionNameAsString().equals(regInfo.getRegionNameAsString());
    }
}
