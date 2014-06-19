package com.splicemachine.hbase.backup;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.SpliceAdmin;

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
		return backup.getBaseBackupFilesystemAsPath()+"/"+getBackupItem();
	}
	
	public void createBackupItemFilesystem() throws IOException {
		FileSystem fileSystem = FileSystem.get(URI.create(getBackupItemFilesystem()),SpliceConstants.config);
		fileSystem.mkdirs(new Path(getBackupItemFilesystem()));		
	}
	public Long getBackupTransactionID() {
		return backup.getBackupTransactionID();
	}
	public String getBackupTransactionIDAsString() {
		return backup.getBackupTransactionIDAsString();
	}
	
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(backup); // TODO Needs to be replaced with protobuf
        out.writeUTF(backupItem);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        backup = (Backup) in.readObject(); // TODO Needs to be replaced with protobuf
        backupItem = in.readUTF();
    }
	
	public void insertBackupItem() throws SQLException {
		Connection connection = null;
		try {
			connection = SpliceAdmin.getDefaultConn();
			PreparedStatement preparedStatement = connection.prepareStatement(String.format(INSERT_BACKUP_ITEM,DEFAULT_SCHEMA,DEFAULT_TABLE));
			preparedStatement.setLong(1, getBackupTransactionID());
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
	
}