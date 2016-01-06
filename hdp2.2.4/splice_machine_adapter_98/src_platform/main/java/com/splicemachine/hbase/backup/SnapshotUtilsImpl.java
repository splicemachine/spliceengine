package com.splicemachine.backup;

import java.io.IOException;
import java.util.*;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;

import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;

public class SnapshotUtilsImpl extends SnapshotUtilsBase {
    static final Logger LOG = Logger.getLogger(SnapshotUtilsImpl.class);

    public List<Object> getFilesForFullBackup(String snapshotName, HRegion region) throws IOException {
        Configuration conf = SpliceConstants.config;
        Path rootDir = new Path(conf.get(HConstants.HBASE_DIR));
        FileSystem fs = rootDir.getFileSystem(conf);
        Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);

        return getSnapshotFilesForRegion(region ,conf, fs, snapshotDir, true);
    }

    /**
     * Extract the list of files (HFiles/HLogs) to copy 
     * @return list of files referenced by the snapshot
     */
    public List<Object> getSnapshotFilesForRegion(final HRegion reg,
                                                  final Configuration conf,
                                                  final FileSystem fs,
                                                  final Path snapshotDir,
                                                  final boolean materialize) throws IOException {
        final String regionName = (reg != null) ? reg.getRegionNameAsString() : null;
        SnapshotDescription snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

        final List<Object> files = new ArrayList<Object>();
        final String table = snapshotDesc.getTable();

        // Get snapshot files
        SpliceLogUtils.info(LOG, "Loading Snapshot '%s' hfile list", snapshotDesc.getName());
        SnapshotReferenceUtil.visitReferencedFiles(fs, snapshotDir,
                new SnapshotReferenceUtil.FileVisitor() {
                    public void storeFile (final String region, final String family, final String hfile)
                            throws IOException {
                        if( regionName == null || isRegionTheSame(regionName, region) ){

                            HFileLink link = createHFileLink(conf, TableName.valueOf(table), region, family, hfile);
                            if (materialize) {
                                SpliceLogUtils.trace(LOG, "hfile: %s", hfile);
                                if (BackupUtils.isReference(hfile)) {
                                    Path p = materializeRefFile(conf, fs, link, reg);
                                    files.add(p);
                                    SpliceLogUtils.info(LOG, "Add %s to snapshot", p);
                                } else {
                                    files.add(link);
                                    SpliceLogUtils.info(LOG, "Add %s to snapshot", link);
                                }
                            } else {
                                files.add(link);
                                SpliceLogUtils.info(LOG, "Add %s to snapshot", link);
                            }
                        }
                    }

                    public void recoveredEdits (final String region, final String logfile)
                            throws IOException {
                        // copied with the snapshot referenecs
                    }

                    public void logFile (final String server, final String logfile)
                            throws IOException {
                        //long size = new HLogLink(conf, server, logfile).getFileStatus(fs).getLen();
                        files.add(new Path(server, logfile));
                    }
                });

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

    public HFileLink getReferredFileLink(HFileLink ref) throws IOException {
        return HFileLink.create(SpliceConstants.config,
                TableName.valueOf(getTableName(ref.getOriginPath()).getBytes()),
                getRegionName(ref.getOriginPath()),
                getColumnFamilyName(ref.getOriginPath()),
                getFileName(ref.getOriginPath()));

    }
    public static HFileLink newLink(Configuration conf,Path linkPath) throws IOException{
        return new HFileLink(conf,linkPath);
    }

    @Override
    public HFileLink createHFileLink(Configuration conf,
                                     TableName table,
                                     String regionName,
                                     String family,
                                     String hfile) throws IOException {
        Path path = HFileLink.createPath(table, regionName, family, hfile);
        HFileLink link = new HFileLink(conf, path);
        return link;
    }
}
