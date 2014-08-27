package com.splicemachine.hbase.backup;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.si.api.TransactionLifecycle;
import com.splicemachine.si.api.Txn;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
/**
 * 
 * Top level Backup Information about the progress of a backup.
 * 
 *
 */
public class Backup implements InternalTable {
	public static final String DEFAULT_SCHEMA = "RECOVERY";
	public static final String DEFAULT_TABLE = "BACKUP";
	public static final String BACKUP_BASE_FOLDER = "BACKUP";
	/**
	 * Backup Scope.  This allows us to understand the scope of the backup.
	 * 
	 * S = Schema
	 * T = Table
	 * D = Database
	 * 
	 */
	public static enum BackupScope {S,T,D};
	/**
	 * S = Success
	 * F = Failure
	 * I = In Process
	 *
	 */
	public static enum BackupStatus {S,F,I}

    public static final String CREATE_SCHEMA = "create schema %s";
	public static final String CREATE_TABLE = "create table %s.%s (backup_transaction_id bigint not null, " + 
	"backup_begin_timestamp timestamp not null, backup_end_timestamp timestamp, backup_status char(1) not null, "
	+ "backup_filesystem varchar(1024) not null, backup_scope char(1) not null, incremental_backup boolean not null, "
	+ "incremental_parent_backup_id bigint, PRIMARY KEY (backup_transaction_id) )";	
	public static final String INSERT_START_BACKUP = "insert into %s.%s (backup_transaction_id, backup_begin_timestamp, backup_status, backup_filesystem, "
			+ "backup_scope, incremental_backup, incremental_parent_backup_id) values (?,?,?,?,?,?,?)";
	public static final String UPDATE_BACKUP_STATUS = "update %s.%s set backup_status = ?, backup_end_timestamp = ? where backup_transaction_id = ?";		
	public static final String RUNNING_CHECK = "select backup_transaction_id from %s.%s where backup_status = ?";	
	public static final String SCHEMA_CHECK = "select schemaid from sys.sysschemas where schemaname = ?";
	public static final String TABLE_CHECK = "select tablename from sys.systables where tablename = ? and schemaid = ?";

	
	private Txn backupTransaction;
	private Timestamp beginBackupTimestamp;
	private Timestamp endBackupTimestamp;	
	private BackupStatus backupStatus;
	private String backupFilesystem;
	private BackupScope backupScope;
	private boolean incrementalBackup;
	private long incrementalParentBackupID;
	private List<BackupItem> backupItems;

	public List<BackupItem> getBackupItems() {
		return backupItems;
	}

	public void setBackupItems(List<BackupItem> backupItems) {
		this.backupItems = backupItems;
	}

	public Backup () {
		
	}
	
	public Txn getBackupTransaction() {
      return backupTransaction;
	}


    public void setBackupTransaction(Txn txn){
        this.backupTransaction = txn;
    }

	public Timestamp getBeginBackupTimestamp() {
		return beginBackupTimestamp;
	}

	public void setBeginBackupTimestamp(Timestamp beginBackupTimestamp) {
		this.beginBackupTimestamp = beginBackupTimestamp;
	}

	public Timestamp getEndBackupTimestamp() {
		return endBackupTimestamp;
	}

	public void setEndBackupTimestamp(Timestamp endBackupTimestamp) {
		this.endBackupTimestamp = endBackupTimestamp;
	}

	public void setIncrementalParentBackupID(long incrementalParentBackupID) {
		this.incrementalParentBackupID = incrementalParentBackupID;
	}

	public BackupStatus getBackupStatus() {
		return backupStatus;
	}

	public void setBackupStatus(BackupStatus backupStatus) {
		this.backupStatus = backupStatus;
	}

	public String getBackupFilesystem() {
		return backupFilesystem;
	}

	public Path getBaseBackupFilesystemAsPath() {
		return new Path(backupFilesystem+"/"+BACKUP_BASE_FOLDER);
	}

	public void setBackupFilesystem(String backupFilesystem) {
		this.backupFilesystem = backupFilesystem;
	}

	public BackupScope getBackupScope() {
		return backupScope;
	}

	public void setBackupScope(BackupScope backupScope) {
		this.backupScope = backupScope;
	}

	public boolean isIncrementalBackup() {
		return incrementalBackup;
	}

	public void setIncrementalBackup(boolean incrementalBackup) {
		this.incrementalBackup = incrementalBackup;
	}

	public Long getIncrementalParentBackupID() {
		return incrementalParentBackupID;
	}

	public void setIncrementalParentBackupID(Long incrementalParentBackupID) {
		this.incrementalParentBackupID = incrementalParentBackupID;
	}
	
	/** Begin Helper Methods 
	 * @throws SQLException **/
	
	public void insertBackup() throws SQLException {
		Connection connection = null;
		try {
			connection = SpliceAdmin.getDefaultConn();
			PreparedStatement preparedStatement = connection.prepareStatement(String.format(INSERT_START_BACKUP,DEFAULT_SCHEMA,DEFAULT_TABLE));
			preparedStatement.setLong(1, backupTransaction.getTxnId());
			preparedStatement.setTimestamp(2, getBeginBackupTimestamp());
			preparedStatement.setString(3, getBackupStatus().toString());
			preparedStatement.setString(4, getBackupFilesystem());
			preparedStatement.setString(5, getBackupScope().toString());
			preparedStatement.setBoolean(6, isIncrementalBackup());
			preparedStatement.setLong(7, getIncrementalParentBackupID());
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
	
	public void writeBackupStatusChange() throws SQLException {
		Connection connection = null;
		try {
			connection = SpliceAdmin.getDefaultConn();
			PreparedStatement preparedStatement = connection.prepareStatement(String.format(UPDATE_BACKUP_STATUS,DEFAULT_SCHEMA,DEFAULT_TABLE));
			preparedStatement.setString(1, getBackupStatus().toString());
			preparedStatement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
			preparedStatement.setLong(3, backupTransaction.getTxnId());
			preparedStatement.executeUpdate();
			return;
		} catch (SQLException e) {
			throw e;
		}  
		finally {
			if (connection !=null)
				connection.close();
		}
	}

	/**
	 * 
	 * Create the initial backup object with a new timestamp cutpoint.  Validates the parent timestamp is before the timestamp obtained from the system.
	 * 
	 * @param backupFileSystem Supported File Systems ?
	 * @param backupScope BackupScope 
	 * @param incrementalParentBackupID Last Incremental Backup
	 * @return
	 * @throws SQLException
	 */	
	public static Backup createBackup(String backupFileSystem, BackupScope backupScope, long incrementalParentBackupID) throws SQLException {
			try {
//          TransactionId transactionId = HTransactorFactory.getTransactionManager().beginTransaction(true);
          Txn backupTxn = TransactionLifecycle.getLifecycleManager().beginTransaction();

          if (incrementalParentBackupID > 0 && incrementalParentBackupID >= backupTxn.getTxnId())
              throw new SQLException(String.format("createBackup attempted to create a backup with an incremental " +
                      "parent id timestamp larger than the current timestamp {incrementalParentBackupID=%d,transactionID=%d",incrementalParentBackupID,backupTxn.getTxnId()));
          backupTxn.elevateToWritable(null);

          Backup backup = new Backup();
          backup.setBackupTransaction(backupTxn);
          backup.setBeginBackupTimestamp(new Timestamp(System.currentTimeMillis()));
          backup.setBackupScope(backupScope);
          backup.setIncrementalBackup(incrementalParentBackupID >= 0);
          backup.setIncrementalParentBackupID(incrementalParentBackupID);
          backup.setBackupStatus(BackupStatus.I);
          backup.setBackupFilesystem(backupFileSystem);
          return backup;
      }
      catch (IOException ioe) {
          throw new SQLException("Could not obtain timestamp",ioe);
      }
  }
    /**
     * Mark the backup status as F
     *
     */
    public void markBackupFailed() {
        this.setBackupStatus(BackupStatus.F);
    }
    /**
     * Mark the backup status as S
     */
    public void markBackupSuccesful() {
        this.setBackupStatus(BackupStatus.S);
    }

    public static void createBackupTable() throws SQLException {
        createBackupTable(DEFAULT_SCHEMA,DEFAULT_TABLE);
    }

    public static void validateBackupSchema() throws SQLException {
        Connection connection = null;
        try {
            connection = SpliceAdmin.getDefaultConn();
            PreparedStatement preparedStatement1 = connection.prepareStatement(SCHEMA_CHECK);
            preparedStatement1.setString(1,DEFAULT_SCHEMA);
            ResultSet schemaResultSet = preparedStatement1.executeQuery();
            if (schemaResultSet.next()) {
                String schemaID = schemaResultSet.getString(1);
                PreparedStatement preparedStatement2 = connection.prepareStatement(TABLE_CHECK);
                preparedStatement2.setString(1, DEFAULT_TABLE);
                preparedStatement2.setString(2, schemaID);
                ResultSet backupResultSet = preparedStatement2.executeQuery();
                if (!backupResultSet.next())
                    Backup.createBackupTable();
                PreparedStatement preparedStatement3 = connection.prepareStatement(TABLE_CHECK);
                preparedStatement3.setString(1, BackupItem.DEFAULT_TABLE);
                preparedStatement3.setString(2, schemaID);

				ResultSet backupItemResultSet = preparedStatement3.executeQuery();
				if (!backupItemResultSet.next())
					BackupItem.createBackupItemTable();
			} else {
				createBackupSchema();
				Backup.createBackupTable();
				BackupItem.createBackupItemTable();
			}
				
			return;
		} catch (SQLException e) {
			throw e;
		}  
		finally {
			if (connection !=null)
				connection.close();
		}
	}
	
	public static void createBackupTable(String schemaName, String tableName) throws SQLException {
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

	public static void createBackupSchema() throws SQLException {
		createBackupSchema(DEFAULT_SCHEMA);
	}
	
	public static void createBackupSchema(String schemaName) throws SQLException {
		Connection connection = null;
		try {
			connection = SpliceAdmin.getDefaultConn();
			PreparedStatement preparedStatement = connection.prepareStatement(String.format(CREATE_SCHEMA,DEFAULT_SCHEMA));
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
	
	
	public static String isBackupRunning() throws SQLException {
		return isBackupRunning(DEFAULT_SCHEMA,DEFAULT_TABLE);
	}
	public static String isBackupRunning(String schemaName, String tableName) throws SQLException {
			Connection connection = null;
			try {
				connection = SpliceAdmin.getDefaultConn();
				PreparedStatement preparedStatement = connection.prepareStatement(String.format(RUNNING_CHECK,schemaName,tableName));
				preparedStatement.setString(1, BackupStatus.I.toString());
				ResultSet rs = preparedStatement.executeQuery();
				while (rs.next())
					return String.format("Current Transaction Backup Is Running %d",rs.getLong(1));
				return null;
			} catch (SQLException e) {
				throw e;
			}  
			finally {
				if (connection !=null)
					connection.close();
			}
		}
	
	public boolean createBaseBackupDirectory() throws IOException, URISyntaxException {
		FileSystem fileSystem = FileSystem.get(URI.create(getBackupFilesystem()),SpliceConstants.config);
		if (!fileSystem.exists(getBaseBackupFilesystemAsPath()))
			fileSystem.mkdirs(getBaseBackupFilesystemAsPath());		
		return true;
	}

	public void addBackupItem(BackupItem backupItem) {
		if (backupItems == null)
			backupItems = new ArrayList<BackupItem>();
		backupItems.add(backupItem);
	}
	
	public void createBackupItems(HBaseAdmin admin) throws IOException {
		HTableDescriptor[] descriptorArray = admin.listTables();
		for (HTableDescriptor descriptor: descriptorArray) {
			addBackupItem(new BackupItem(descriptor,this));
		}
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      if(true)
          throw new UnsupportedOperationException("DECODE TRANSACTION");
		backupStatus = BackupStatus.valueOf(in.readUTF());
		backupFilesystem = in.readUTF();
		backupScope = BackupScope.valueOf(in.readUTF());
		incrementalBackup = in.readBoolean();
		incrementalParentBackupID = in.readLong();		
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
      if(true)
          throw new UnsupportedOperationException("ENCODE TRANSACTION");
		out.writeUTF(backupStatus.toString());
		out.writeUTF(backupFilesystem);
		out.writeUTF(backupScope.toString());
		out.writeBoolean(incrementalBackup);
		out.writeLong(incrementalParentBackupID);
	}
}