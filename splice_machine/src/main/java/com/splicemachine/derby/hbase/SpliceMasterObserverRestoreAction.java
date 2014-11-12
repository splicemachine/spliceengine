package com.splicemachine.derby.hbase;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Restore tables.
 */
public class SpliceMasterObserverRestoreAction {
	public static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
    public  static final String BACKUP_PATH = "BACKUP_PATH";
    private static Logger LOG = Logger.getLogger(SpliceMasterObserver.class);
    private MasterServices masterServices;

    public SpliceMasterObserverRestoreAction(MasterServices masterServices) {
        this.masterServices = masterServices;
    }

    public void restoreDatabase(HTableDescriptor desc) throws IOException {
        String backupDirectory = desc.getValue(BACKUP_PATH);
        SpliceLogUtils.info(LOG, "restoring database, dir=" + backupDirectory);
        MasterFileSystem fileSystemManager = masterServices.getMasterFileSystem();
        CatalogTracker catalogTracker = masterServices.getCatalogTracker();
        FileSystem fs = fileSystemManager.getFileSystem();
        Map<String, HTableDescriptor> descriptors = masterServices.getTableDescriptors().getAll();
        for (String tableName : descriptors.keySet()) {
            if (LOG.isInfoEnabled())
                SpliceLogUtils.info(LOG, "Deleting table descriptor " + tableName);
            derbyFactory.removeTableFromDescriptors(masterServices, tableName);
        }


        FileStatus[] tableDirectories = FSUtils.listStatus(fs, new Path(backupDirectory + "/BACKUP"));
        if (tableDirectories == null) return;

        for (FileStatus tableDir : tableDirectories) {
            if (LOG.isInfoEnabled())
                SpliceLogUtils.info(LOG, "Restoring table descriptor for %s", tableDir.getPath().getName());
            HTableDescriptor td = loadTableDescriptorFromFile(tableDir.getPath().toString() + "/.tableinfo");
            if (LOG.isInfoEnabled())
                SpliceLogUtils.info(LOG, "Restoring table descriptor for %s {%s}", tableDir.getPath().getName(), td.getNameAsString());
            masterServices.getTableDescriptors().add(td);
            FileStatus[] regionDirectories = FSUtils.listStatus(fs, tableDir.getPath(), new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    return !path.getName().startsWith(".");
                }

            });
            List<HRegionInfo> regions = new ArrayList<HRegionInfo>();
            for (FileStatus regionDir : regionDirectories) {
                if (LOG.isInfoEnabled())
                    SpliceLogUtils.info(LOG, "Restoring region" + regionDir.getPath().toString());
                HRegionInfo hri = derbyFactory.loadRegionInfoFileContent(fs, regionDir.getPath());
                if (LOG.isInfoEnabled())
                    SpliceLogUtils.info(LOG, "Restoring region with hri " + hri);
                regions.add(hri);
            }
            MetaEditor.mutateRegions(catalogTracker, new ArrayList<HRegionInfo>(), regions);

        }
/*    	    	regions.add(regionDir.getPath().getName());
            }


    	    for (FileStatus regionDir: regionDirs) {
    	        HRegionInfo hri = HRegion.loadDotRegionInfoFileContent(fs, regionDir.getPath());
    	        regions.add(hri);
    	      }

  */



        	/*
            byte[] tableName = hTableDescriptor.getName();
    	    Path tableDir = HTableDescriptor.getTableDir(rootDir, tableName);

    	    try {
    	      // 1. Update descriptor
    	      this.masterServices.getTableDescriptors().add(hTableDescriptor);

    	      // 2. Execute the on-disk Restore
    	      LOG.debug("Starting restore snapshot=" + SnapshotDescriptionUtils.toString(snapshot));
    	      Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshot, rootDir);
    	      RestoreSnapshotHelper restoreHelper = new RestoreSnapshotHelper(
    	          masterServices.getConfiguration(), fs,
    	          snapshot, snapshotDir, hTableDescriptor, tableDir, monitor, status);
    	      RestoreSnapshotHelper.RestoreMetaChanges metaChanges = restoreHelper.restoreHdfsRegions();

    	      // 3. Applies changes to .META.
    	      hris.clear();
    	      status.setStatus("Preparing to restore each region");
    	      if (metaChanges.hasRegionsToAdd()) hris.addAll(metaChanges.getRegionsToAdd());
    	      if (metaChanges.hasRegionsToRestore()) hris.addAll(metaChanges.getRegionsToRestore());
    	      List<HRegionInfo> hrisToRemove = metaChanges.getRegionsToRemove();
    	      MetaEditor.mutateRegions(catalogTracker, hrisToRemove, hris);

    	      // At this point the restore is complete. Next step is enabling the table.
    	      LOG.info("Restore snapshot=" + SnapshotDescriptionUtils.toString(snapshot) + " on table=" +
    	        Bytes.toString(tableName) + " completed!");
    	    } catch (IOException e) {
    	      String msg = "restore snapshot=" + SnapshotDescriptionUtils.toString(snapshot)
    	          + " failed. Try re-running the restore command.";
    	      LOG.error(msg, e);
    	      monitor.receive(new ForeignException(masterServices.getServerName().toString(), e));
    	      throw new RestoreSnapshotException(msg, e);
    	    } finally {
    	      this.stopped = true;
    	    }
    	*/

    }

    public HTableDescriptor loadTableDescriptorFromFile(String fileToLoad) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "loadTableDescriptorFromFile %s", fileToLoad);
        FileSystem fs = FileSystem.get(URI.create(fileToLoad), SpliceConstants.config);
        FSDataInputStream in = fs.open(new Path(fileToLoad));
        HTableDescriptor td = new HTableDescriptor();
        td.readFields(in);
        return td;
    }
}
