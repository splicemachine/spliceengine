package com.splicemachine.hbase.backup;

import java.io.*;
import java.net.URI;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CancellationException;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.job.JobFuture;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.Txn;
import com.splicemachine.db.iapi.error.StandardException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import org.apache.hadoop.hbase.util.Pair;

import org.apache.log4j.Logger;

public class BackupItem implements InternalTable {

    private static Logger LOG = Logger.getLogger(BackupItem.class);

    private static int SNAPSHOT_MAX_RETRY = 10;
	public static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;

    public BackupItem () {
	}
	
	public BackupItem(HTableDescriptor tableDescriptor, Backup backup) {
		this.backupItem = tableDescriptor.getNameAsString();
		this.backup = backup;
		this.tableDescriptor = tableDescriptor;
	}
	
	private Backup backup;
	private String backupItem;
	private Timestamp backupItemBeginTimestamp;
	private Timestamp backupItemEndTimestamp;
    private String snapshotName;
    private String lastSnapshotName;
    private List<Pair<Long, Long>> ignoreTxnList;

    private List<RegionInfo> regionInfoList = new ArrayList<RegionInfo>();

    public HTableDescriptor getTableDescriptor() {
        return tableDescriptor;
    }

    private HTableDescriptor tableDescriptor;
	
	public Backup getBackup() {
		return backup;
	}
	public void setBackup(Backup backup) {
		this.backup = backup;
	}
	public String getBackupItem() {
		return backupItem;
	}
	public byte[] getBackupItemBytes() {
		return Bytes.toBytes(backupItem);
	}
    public List<RegionInfo> getRegionInfoList() {
        return regionInfoList;
    }

	public void setBackupItem(String backupItem) {
		this.backupItem = backupItem;
	}
	
	public Timestamp getBackupItemBeginTimestamp() {
		return backupItemBeginTimestamp;
	}

	public void setBackupItemBeginTimestamp(Timestamp backupItemBeginTimestamp) {
		this.backupItemBeginTimestamp = backupItemBeginTimestamp;
	}

	public Timestamp getBackupItemEndTimestamp() {
		return backupItemEndTimestamp;
	}

	public void setBackupItemEndTimestamp(Timestamp backupItemEndTimestamp) {
		this.backupItemEndTimestamp = backupItemEndTimestamp;
	}

	public String getBackupItemFilesystem() {
		return backup.getTableBackupFilesystemAsPath()+"/"+getBackupItem();
	}

    public Txn getBackupTransaction(){
        return backup.getBackupTransaction();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(backup); // TODO Needs to be replaced with protobuf
        out.writeUTF(backupItem);
        out.writeObject(regionInfoList);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        backup = (Backup) in.readObject(); // TODO Needs to be replaced with protobuf
        backupItem = in.readUTF();
        regionInfoList = (List<RegionInfo>) in.readObject();
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public void setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }

    public void insertBackupItem() throws StandardException {
        try {
            BackupSystemProcedures.backupItemReporter.report(this, getBackupTransaction());
        }
        catch (Exception e) {
            throw StandardException.newException(e.getMessage());
        }
    }

    public void recreateItem(HBaseAdmin admin) throws IOException {
        byte[][] splitKeys = computeSplitKeys();
        admin.createTable(getTableDescriptor(), splitKeys);
    }

    private byte[][] computeSplitKeys() {
        byte[][] splits = new byte[regionInfoList.size() - 1][]; // 1 region = no splits, 2 regions = 1 split, etc.
        int i = 0;
        for (RegionInfo regionInfo : regionInfoList) {
            if (regionInfo.getHRegionInfo().getEndKey().length != 0) {
                // add all endKeys except for the last region
                splits[i] = regionInfo.getHRegionInfo().getEndKey();
                i++;
            }
        }
        // sort splits
        Arrays.sort(splits, new Bytes.ByteArrayComparator());
        return splits;
    }

	public void writeDescriptorToFileSystem() throws IOException {
		FileSystem fs = FileSystem.get(URI.create(getBackupItemFilesystem()), SpliceConstants.config);
		FSDataOutputStream out = fs.create(new Path(getBackupItemFilesystem()+"/.tableinfo"));
		tableDescriptor.write(out);
		out.flush();
		out.close();
	}

    public void readDescriptorFromFileSystem() throws IOException {
        FileSystem fs = FileSystem.get(URI.create(getBackupItemFilesystem()),SpliceConstants.config);
        FSDataInputStream in = fs.open(new Path(getBackupItemFilesystem() + "/.tableinfo"));
        tableDescriptor = new HTableDescriptor();
        tableDescriptor.readFields(in);
        in.close();
        readRegionsFromFileSystem(fs);
    }

    private void readRegionsFromFileSystem(FileSystem fs) throws IOException {
        FileStatus[] status = fs.listStatus(new Path(getBackupItemFilesystem()));

        for (FileStatus stat : status) {
            if (!stat.isDir() || stat.getPath().getName().compareTo(".tmp") == 0) {
                continue; // ignore non directories
            }
            HRegionInfo regionInfo = derbyFactory.loadRegionInfoFileContent(fs, stat.getPath());
            addRegionInfo(new RegionInfo(regionInfo, getFamilyPaths(fs, stat.getPath())));
        };
    }

    public List<Pair<Long, Long>> getIgnoreTxns () {
        return ignoreTxnList;
    }

    public void addIgnoreTxn() {
        if (ignoreTxnList == null) {
            ignoreTxnList = new ArrayList<>();
        }
        Backup backup = getBackup();
        ignoreTxnList.add(Pair.newPair(backup.getBackupId(), backup.getTimestampSource()));
    }

    public void addIgnoreTxn(Pair<Long, Long> p) {
        ignoreTxnList.add(p);
    }

    private List<Pair<byte[],String>> getFamilyPaths(FileSystem fs, Path root) throws IOException {
        List<Pair<byte[], String>> famPaths = new ArrayList<Pair<byte[], String>>();
        FileStatus[] status = fs.listStatus(root);
        for (FileStatus stat : status) {
            String name = stat.getPath().getName();
            // We only care about Splice families, "V" and "P"
            if (name.equals(Bytes.toString(SpliceConstants.DEFAULT_FAMILY_BYTES))
                    || name.equals(SpliceConstants.SI_PERMISSION_FAMILY)) {
                // we have a family directory, the hfile is inside it
                byte[] family = Bytes.toBytes(stat.getPath().getName());

                FileStatus[] familyStatus = fs.listStatus(stat.getPath());
                for (FileStatus familyStat : familyStatus) {
                    if (familyStat.getPath().getName().startsWith(".")) {
                        continue; // ignore CRCs
                    }
                    famPaths.add(new Pair<byte[], String>(family, familyStat.getPath().toString()));
                }
            }
        }
        return famPaths;
    }

    private void addRegionInfo(RegionInfo regionInfo) {
        regionInfoList.add(regionInfo);
    }

    public void setLastSnapshotName(Set<String> snapshotNameSet) throws StandardException {

        if (snapshotNameSet.size() == 0)
            return;

        long parentBackupId = backup.getParentBackupId();
        if (parentBackupId > 0) {
            String name = backupItem + "_" + parentBackupId;
            if (snapshotNameSet.contains(name)) {
                lastSnapshotName = name;
            }
        }
    }

    public boolean doBackup() throws StandardException {
        JobInfo info = null;
        boolean backedUp = false;
        try {
            setBackupItemBeginTimestamp(new Timestamp(System.currentTimeMillis()));
            HTableInterface table = SpliceAccessManager.getHTable(getBackupItemBytes());

            if (backup.getParentBackupId() > 0) {
                CreateIncrementalBackupJob job =
                        new CreateIncrementalBackupJob(this, table, getBackupItemFilesystem(),
                                snapshotName, lastSnapshotName);
                JobFuture future = SpliceDriver.driver().getJobScheduler().submit(job);
                info = new JobInfo(job.getJobId(), future.getNumTasks(), System.currentTimeMillis());
                info.setJobFuture(future);
                future.completeAll(info);
            }
            else {
                CreateBackupJob job = new CreateBackupJob(this, table, getBackupItemFilesystem());
                JobFuture future = SpliceDriver.driver().getJobScheduler().submit(job);
                info = new JobInfo(job.getJobId(), future.getNumTasks(), System.currentTimeMillis());
                info.setJobFuture(future);
                future.completeAll(info);
            }

            // Check backup directory for this item. If it does not exists, nothing was backed up incrementally.
            // Remove the entry for this item in backup.backup_items table
            FileSystem fileSystem = FileSystem.get(URI.create(getBackupItemFilesystem()),SpliceConstants.config);
            Path path = new Path(getBackupItemFilesystem());
            FileStatus[] status = fileSystem.listStatus(path);
            if (backup.isIncrementalBackup() && !containsRegion(status)) {
                fileSystem.delete(path, true);
                backedUp = false;
            }
            else {
                writeDescriptorToFileSystem();
                setBackupItemEndTimestamp(new Timestamp(System.currentTimeMillis()));
                insertBackupItem();
                backedUp = true;
            }
        } catch (CancellationException ce) {
            throw Exceptions.parseException(ce);
        } catch (Exception e) {
            if (info != null) info.failJob();
            throw Exceptions.parseException(e);
        }

        return backedUp;
    }

    public boolean containsRegion(FileStatus[] fileStatus) {

        int count = 0;
        for (FileStatus status : fileStatus) {
            if (status.getPath().getName().charAt(0) == '.') {
                continue;
            }
            count++;
        }

        return count > 0;
    }

    public void createSnapshot(HBaseAdmin admin, long snapId, Set<String> snapshotNameSet, int retry) throws StandardException {

        try {
            long start = System.currentTimeMillis();
            snapshotName = tableDescriptor.getNameAsString() + "_" + snapId;
            admin.snapshot(snapshotName.getBytes(), tableDescriptor.getName());
            snapshotNameSet.add(snapshotName);
            LOG.info("Snapshot: " + tableDescriptor.getNameAsString() + " done in " + (System.currentTimeMillis() - start) + "ms");
        }
        catch (Exception e) {
            retry++;
            if (retry > SNAPSHOT_MAX_RETRY) {
                throw Exceptions.parseException(e);
            }
            else {
                try {
                    Thread.sleep(10000);
                } catch (Exception ex) {
                    throw Exceptions.parseException(ex);
                }
                createSnapshot(admin, snapId, snapshotNameSet, retry);
            }
        }
    }

    public String getLastSnapshotName() {
        return lastSnapshotName;
    }

    public void setBackupId(long backupId) {
        backup.setBackupId(backupId);
    }

    public long getBackupId() {
        return backup.getBackupId();
    }

    public static class RegionInfo implements Externalizable {
        private HRegionInfo hRegionInfo;
        private List<Pair<byte[], String>> famPaths;

        public RegionInfo() {
        }

        public RegionInfo(HRegionInfo hRegionInfo, List<Pair<byte[], String>> famPaths) {
            this.hRegionInfo = hRegionInfo;
            this.famPaths = famPaths;
        }

        public HRegionInfo getHRegionInfo() {
            return hRegionInfo;
        }

        public void setHRegionInfo(HRegionInfo hRegionInfo) {
            this.hRegionInfo = hRegionInfo;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            hRegionInfo.write(out);
            out.writeObject(famPaths);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            hRegionInfo = new HRegionInfo();
            hRegionInfo.readFields(in);
            famPaths = (List<Pair<byte[], String>>) in.readObject();
        }

        public List<Pair<byte[], String>> getFamPaths() {
            return famPaths;
        }
    }
}