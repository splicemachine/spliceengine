package com.splicemachine.hbase.backup;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotFileInfo;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotFileInfoOrBuilder;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SnapshotUtilsImpl extends SnapshotUtilsBase {
    static final Logger LOG = Logger.getLogger(SnapshotUtilsImpl.class);

    @Override
    public List<Object> getFilesForFullBackup(String snapshotName, HRegion region) throws IOException {
        Configuration conf = SpliceConstants.config;
        Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
        FileSystem fs = rootDir.getFileSystem(conf);
        Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);

        return getSnapshotFilesForRegion(region, conf, fs, snapshotDir, true);
    }

    /**
     * Extract the list of files (HFiles/HLogs) to copy using Map-Reduce.
     *
     * @return list of files referenced by the snapshot (pair of path and size)
     */
    public List<Object> getSnapshotFilesForRegion(final HRegion region,
                                                  final Configuration conf,
                                                  final FileSystem fs,
                                                  final Path snapshotDir,
                                                  final boolean materialize) throws IOException {
        SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

        final List<Object> files = new ArrayList<Object>();
        final TableName table = TableName.valueOf(snapshotDesc.getTable());

        // Get snapshot files
        SpliceLogUtils.info(LOG, "Loading Snapshot '%s' hfile list", snapshotDesc.getName());
        SnapshotReferenceUtil.visitReferencedFiles(conf, fs, snapshotDir, snapshotDesc,
                new SnapshotReferenceUtil.SnapshotVisitor() {
                    @Override
                    public void storeFile(final HRegionInfo regionInfo, final String family,
                                          final SnapshotRegionManifest.StoreFile storeFile) throws IOException {

                        boolean isReference = storeFile.hasReference();

                        String regionName = regionInfo.getEncodedName();
                        if (region != null && isCurrentRegion(region, regionInfo) == false) {
                            // If not current return
                            return;
                        }
                        String hfile = storeFile.getName();
                        HFileLink filePath = createHFileLink(conf, table, regionName, family, hfile);
                        if (materialize) {
                            if (isReference) {
                                // If its materialized reference - we add Path
                                Path p = materializeRefFile(conf, fs, filePath, region);
                                files.add(p);
                                SpliceLogUtils.info(LOG, "Add %s to snapshot", p);
                            } else {
                                // Otherwise we add HFileLink
                                files.add(filePath);
                                SpliceLogUtils.info(LOG, "Add %s to snapshot", filePath);
                            }
                        }
                        else {
                            files.add(filePath);
                        }

                    }

                    @Override
                    public void logFile(final String server, final String logfile)
                            throws IOException {
                        SnapshotFileInfo fileInfo = SnapshotFileInfo.newBuilder()
                                .setType(SnapshotFileInfo.Type.WAL)
                                .setWalServer(server)
                                .setWalName(logfile)
                                .build();
                        files.add(getFilePath(fileInfo));
                    }
                }
        );

        return files;
    }

    @Override
    public List<Object> getSnapshotFilesForRegion(final HRegion region,
                                                  final Configuration conf,
                                                  final FileSystem fs,
                                                  final String snapshotName,
                                                  final boolean materialize) throws IOException {
        Path rootDir = FSUtils.getRootDir(conf);

        Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
        List<Object> paths = getSnapshotFilesForRegion(region, conf, fs, snapshotDir, materialize);

        return paths;
    }

    /**
     * Returns path to a file referenced in a snapshot
     * FIXME: race condition possible, if file gets archived during
     * backup operation. We should probably return HFileLink and process
     * this link accordingly.
     *
     * @param fileInfo
     * @return path to a referenced file
     * @throws IOException
     */

    // for testing
    public HFileLink getFilePath(final SnapshotFileInfoOrBuilder fileInfo) throws IOException {
        try {
            //Configuration conf = SpliceConstants.config;
            //Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
            //FileSystem fs = rootDir.getFileSystem(conf);        	  
            HFileLink link = null;
            switch (fileInfo.getType()) {
                case HFILE:
                    Path inputPath = new Path(fileInfo.getHfile());
                    link = HFileLink.buildFromHFileLinkPattern(SpliceConstants.config, inputPath);
                    break;
                case WAL:
                    LOG.warn("Unexpected log file in a snapshot: " + fileInfo.toString());
                    break;
                default:
                    throw new IOException("Invalid File Type: " + fileInfo.getType().toString());
            }

            return link;

        } catch (IOException e) {
            LOG.error("Unable to get the status for source file=" + fileInfo.toString(), e);
            throw e;
        }
    }

    public static HFileLink newLink(Configuration conf, Path path) throws IOException {
        return HFileLink.buildFromHFileLinkPattern(conf, path);
    }


    public HFileLink getReferredFileLink(HFileLink ref) throws IOException {
        return HFileLink.build(SpliceConstants.config,
                TableName.valueOf(getTableName(ref.getOriginPath()).getBytes()),
                getRegionName(ref.getOriginPath()),
                getColumnFamilyName(ref.getOriginPath()),
                getFileName(ref.getOriginPath()));

    }

    @Override
    public HFileLink createHFileLink(Configuration conf,
                                     TableName table,
                                     String regionName,
                                     String family,
                                     String hfile) throws IOException {
        Path path = HFileLink.createPath(table, regionName, family, hfile);

        SnapshotFileInfo fileInfo = SnapshotFileInfo.newBuilder()
                .setType(SnapshotFileInfo.Type.HFILE)
                .setHfile(path.toString())
                .build();

        HFileLink filePath = getFilePath(fileInfo);
        return filePath;
    }
}