package com.splicemachine.hbase.backup;

import java.io.*;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.splicemachine.si.api.Txn;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.SpliceAdmin;
import org.apache.hadoop.hbase.util.Pair;

public class BackupItem implements InternalTable {
	public static final String DEFAULT_SCHEMA = "RECOVERY";
	public static final String DEFAULT_TABLE = "BACKUP_ITEM";
	public static final String CREATE_TABLE = "create table %s.%s (backup_transaction_id bigint not null, " + 
	"backup_item varchar(1024) not null, backup_begin_timestamp timestamp not null, backup_end_timestamp timestamp, PRIMARY KEY (backup_transaction_id, backup_item) )";		
	public static final String INSERT_BACKUP_ITEM = "insert into table %s.%s (backup_transaction_id, backup_item, backup_begin_timestamp)"
			+ " values (?,?,?)";
	
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
	
	public void createBackupItemFilesystem() throws IOException {
		FileSystem fileSystem = FileSystem.get(URI.create(getBackupItemFilesystem()),SpliceConstants.config);
		fileSystem.mkdirs(new Path(getBackupItemFilesystem()));		
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
	
	public void insertBackupItem() throws SQLException {
		Connection connection = null;
		try {
			connection = SpliceAdmin.getDefaultConn();
			PreparedStatement preparedStatement = connection.prepareStatement(String.format(INSERT_BACKUP_ITEM,DEFAULT_SCHEMA,DEFAULT_TABLE));
			preparedStatement.setLong(1, getBackupTransaction().getTxnId());
			preparedStatement.setString(2, getBackupItem());
			preparedStatement.setTimestamp(3, getBackupItemBeginTimestamp());	
			preparedStatement.execute();
			return;
		} catch (SQLException e) {
			throw e;
		}  
		finally {
			if (connection !=null)
				connection.close();
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

    public static void createBackupItemTable() throws SQLException {
		createBackupItemTable(DEFAULT_TABLE, DEFAULT_SCHEMA);
	}
	
	public static void createBackupItemTable(String tableName, String schemaName) throws SQLException {
		Connection connection = null;
		try {
			connection = SpliceAdmin.getDefaultConn();
			PreparedStatement preparedStatement = connection.prepareStatement(String.format(CREATE_TABLE,schemaName,tableName));
			preparedStatement.execute();
			return;
		} catch (SQLException e) {
			throw e;
		}  
		finally {
			if (connection !=null)
				connection.close();
		}
	}
    
	public void writeDescriptorToFileSystem() throws IOException {
		FileSystem fs = FileSystem.get(URI.create(getBackupItemFilesystem()),SpliceConstants.config);
		FSDataOutputStream out = fs.create(new Path(getBackupItemFilesystem()+"/.tableinfo"));
		tableDescriptor.write(out);
		out.flush();
		out.close();
		fs.close();
	}

    public void readDescriptorFromFileSystem() throws IOException {
        FileSystem fs = FileSystem.get(URI.create(getBackupItemFilesystem()),SpliceConstants.config);
        FSDataInputStream in = fs.open(new Path(getBackupItemFilesystem() + "/.tableinfo"));
        tableDescriptor = new HTableDescriptor();
        tableDescriptor.readFields(in);
        in.close();
        readRegionsFromFileSystem(fs);
        fs.close();
    }

    private void readRegionsFromFileSystem(FileSystem fs) throws IOException {
        FileStatus[] status = fs.listStatus(new Path(getBackupItemFilesystem()));
        for (FileStatus stat : status) {
            if (!stat.isDir()) {
                continue; // ignore non directories
            }
            HRegionInfo regionInfo = HRegion.loadDotRegionInfoFileContent(fs, stat.getPath());
            addRegionInfo(new RegionInfo(regionInfo, getFamilyPaths(fs, stat.getPath())));
        };
        fs.close();
    }

    private List<Pair<byte[],String>> getFamilyPaths(FileSystem fs, Path root) throws IOException {
        List<Pair<byte[], String>> famPaths = new ArrayList<Pair<byte[], String>>();
        FileStatus[] status = fs.listStatus(root);
        for (FileStatus stat : status) {
            if (!stat.isDir() || stat.getPath().getName().equals("region.info")) {
                continue; // ignore non directories
            }
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
        return famPaths;
    }

    private void addRegionInfo(RegionInfo regionInfo) {
        regionInfoList.add(regionInfo);
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