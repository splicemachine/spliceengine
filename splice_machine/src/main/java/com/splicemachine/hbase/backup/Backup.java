package com.splicemachine.hbase.backup;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.ConglomerateUtils;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.si.impl.TransactionTimestamps;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.ZkUtils;

import com.splicemachine.db.iapi.error.StandardException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.*;

/**
 *
 * Top level Backup Information about the progress of a backup.
 *
 *
 */
public class Backup implements InternalTable {
    private static Logger LOG = Logger.getLogger(Backup.class);

    public static final String DEFAULT_SCHEMA = "BACKUP";
    public static final String DEFAULT_TABLE = "BACKUP";
    public static final String BACKUP_BASE_FOLDER = "BACKUP";
    public static final String BACKUP_TEMP_BASE_FOLDER = "BACKUP_TEMP";
    public static final String BACKUP_OLD_BASE_FOLDER = "BACKUP_OLD";
    public static final String BACKUP_TABLE_FOLDER = "tables";
    public static final String BACKUP_META_FOLDER = "meta";
    public static final String BACKUP_PROPERTIES_FOLDER = "properties";
    private long timestampSource;

    private boolean temporaryBaseFolder; // true if not using default base folder



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
            + "incremental_parent_backup_id bigint, backup_items int, PRIMARY KEY (backup_transaction_id) )";
    public static final String INSERT_START_BACKUP = "insert into %s.%s (backup_transaction_id, backup_begin_timestamp, backup_status, backup_filesystem, "
            + "backup_scope, incremental_backup, incremental_parent_backup_id, backup_items) values (?,?,?,?,?,?,?,?)";
    public static final String UPDATE_BACKUP_STATUS = "update %s.%s set backup_status = ?, backup_end_timestamp = ? where backup_transaction_id = ?";
    public static final String RUNNING_CHECK = "select backup_transaction_id from %s.%s where backup_status = ?";
    public static final String SCHEMA_CHECK = "select schemaid from sys.sysschemas where schemaname = ?";
    public static final String TABLE_CHECK = "select tablename from sys.systables where tablename = ? and schemaid = ?";
    public static final String QUERY_PARENT_BACKUP_DIRECTORY = "select backup_filesystem from %s.%s where backup_transaction_id = ?";
    public static final String QUERY_PARENT_BACKUP = "select * from %s.%s where backup_transaction_id=?";

    public static final String VERSION_FILE = "version";
    public static final String BACKUP_TIMESTAMP_FILE = "backupTimestamp";
    public static final String TIMESTAMP_SOURCE_FILE = "timestampSource";
    public static final String CONGLOMERATE_SEQUENCE_FILE = "conglomerateSequence";

    private Txn backupTransaction;
    private Timestamp beginBackupTimestamp;
    private Timestamp endBackupTimestamp;
    private BackupStatus backupStatus;
    private String backupFilesystem;
    private String baseFolder;
    private BackupScope backupScope;
    private boolean incrementalBackup;
    private long incrementalParentBackupID;
    private Backup incrementalParentBackup;
    private HashMap<String, BackupItem> backupItems;
    private long backupTimestamp;

    public String getBackupVersion() {
        return backupVersion;
    }

    public void setBackupVersion(String backupVersion) {
        this.backupVersion = backupVersion;
    }

    public long getBackupTimestamp() {
        return backupTimestamp;
    }

    public void setBackupTimestamp(long backupTimestamp) {
        this.backupTimestamp = backupTimestamp;
    }

    private String backupVersion;

    public HashMap<String, BackupItem> getBackupItems() {
        return backupItems;
    }

    public void setBackupItems(HashMap<String, BackupItem> backupItems) {
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
        return new Path(baseFolder);
    }

    public Path getTableBackupFilesystemAsPath() {
        return new Path(baseFolder+"/"+BACKUP_TABLE_FOLDER);
    }
    public Path getMetaBackupFilesystemAsPath() {
        return new Path(baseFolder+"/"+BACKUP_META_FOLDER);
    }
    public Path getPropertiesBackupFilesystemAsPath() {
        return new Path(baseFolder+"/"+BACKUP_PROPERTIES_FOLDER);
    }

    public void setBackupFilesystem(String backupFilesystem) {
        this.backupFilesystem = backupFilesystem;
        this.baseFolder = backupFilesystem + "/" + BACKUP_BASE_FOLDER;
        if (incrementalParentBackupID != -1) {
            this.baseFolder += "$" + incrementalParentBackupID;
        }
        this.baseFolder += "_" + backupTransaction.getBeginTimestamp();
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
            preparedStatement.setInt(8, backupItems.size());
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

    private void initIncrementalParentBackup(long incrementalParentBackupID) throws SQLException {

        if (incrementalParentBackupID <= 0) {
            // no-op for the top level full backup
            return;
        }

        Connection connection = null;
        try {
            connection = SpliceAdmin.getDefaultConn();
            PreparedStatement preparedStatement = connection.prepareStatement(String.format(QUERY_PARENT_BACKUP,DEFAULT_SCHEMA,DEFAULT_TABLE));
            preparedStatement.setLong(1, incrementalParentBackupID);
            ResultSet rs = preparedStatement.executeQuery();
            if (!rs.next()) {
                // Return if there is no such parent backup
                SpliceLogUtils.warn(LOG, "Cannot find parent backup with ID %d", incrementalParentBackupID);
                return;
            }
            incrementalParentBackup = new Backup();
            rs.close();

            preparedStatement = connection.prepareStatement(
                    String.format(BackupItem.QUERY_BACKUP_ITEM,BackupItem.DEFAULT_SCHEMA,BackupItem.DEFAULT_TABLE));
            preparedStatement.setLong(1, incrementalParentBackupID);
            rs = preparedStatement.executeQuery();
            while(rs.next()) {
                BackupItem backupItem = new BackupItem();
                backupItem.setBackup(incrementalParentBackup);
                backupItem.setBackupItem(rs.getString("BACKUP_ITEM"));
                backupItem.setBackupItemBeginTimestamp(rs.getTimestamp("BACKUP_BEGIN_TIMESTAMP"));
                backupItem.setBackupItemEndTimestamp(rs.getTimestamp("BACKUP_END_TIMESTAMP"));
                incrementalParentBackup.addBackupItem(backupItem);
            }

        } catch (SQLException e) {
            throw e;
        }
        finally {
            if (connection !=null)
                connection.close();
        }
    }

    public Backup getIncrementalParentBackup() {
        return incrementalParentBackup;
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
            backupTxn.elevateToWritable("recovery".getBytes());

            Backup backup = new Backup();
            backup.setBackupTransaction(backupTxn);
            backup.setBeginBackupTimestamp(new Timestamp(System.currentTimeMillis()));
            backup.setBackupScope(backupScope);
            backup.setIncrementalBackup(incrementalParentBackupID >= 0);
            backup.setIncrementalParentBackupID(incrementalParentBackupID);
            backup.setBackupStatus(BackupStatus.I);
            backup.setBackupFilesystem(backupFileSystem);
            backup.setBackupTimestamp(backupTxn.getBeginTimestamp());
            backup.setBackupVersion("1");
            backup.initIncrementalParentBackup(incrementalParentBackupID);
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
        if (!fileSystem.exists(getBaseBackupFilesystemAsPath())) {
            fileSystem.mkdirs(getBaseBackupFilesystemAsPath());
        } else {
            // a backup already exists, use a temporary
            temporaryBaseFolder = true;
            baseFolder = backupFilesystem + "/" + BACKUP_TEMP_BASE_FOLDER;
        }
        if (!fileSystem.exists(getTableBackupFilesystemAsPath()))
            fileSystem.mkdirs(getTableBackupFilesystemAsPath());
        if (!fileSystem.exists(getMetaBackupFilesystemAsPath()))
            fileSystem.mkdirs(getMetaBackupFilesystemAsPath());
        if (!fileSystem.exists(getPropertiesBackupFilesystemAsPath()))
            fileSystem.mkdirs(getPropertiesBackupFilesystemAsPath());
        return true;
    }

    public void addBackupItem(BackupItem backupItem) {
        if (backupItems == null)
            backupItems = new HashMap<String, BackupItem>();
        backupItems.put(backupItem.getBackupItem(), backupItem);
    }

    public void readBackupItems() throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(getBackupFilesystem()),SpliceConstants.config);
        FileStatus[] status = fileSystem.listStatus(getTableBackupFilesystemAsPath());
        for (FileStatus stat : status) {
            BackupItem item = new BackupItem();
            item.setBackup(this);
            item.setBackupItem(stat.getPath().getName());
            item.readDescriptorFromFileSystem();
            addBackupItem(item);
        };
    }

    public void createBackupItems(HBaseAdmin admin) throws IOException, StandardException {
        HTableDescriptor[] descriptorArray = admin.listTables();
        for (HTableDescriptor descriptor: descriptorArray) {
            BackupItem item = new BackupItem(descriptor,this);
            item.setLastBackupTimestamp();
            addBackupItem(item);
        }
    }

    public static Backup readBackup(String backupFileSystem, BackupScope backupScope) throws SQLException, IOException, StandardException {
        Txn backupTxn = TransactionLifecycle.getLifecycleManager().beginTransaction().elevateToWritable("recovery".getBytes());
        Backup backup = new Backup();
        backup.setBeginBackupTimestamp(new Timestamp(System.currentTimeMillis()));
        backup.setBackupScope(backupScope);
        backup.setBackupTransaction(backupTxn);
//            backup.setIncrementalBackup(incrementalParentBackupID >= 0);
//            backup.setIncrementalParentBackupID(incrementalParentBackupID);
        backup.setBackupStatus(BackupStatus.I);
        backup.setBackupFilesystem(backupFileSystem);
        backup.readBackupItems();
        backup.restoreProperties();
        backup.restoreMetadata();
        return backup;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//  FIXME reenable throw     if(true)
//          throw new UnsupportedOperationException("DECODE TRANSACTION");
        backupStatus = BackupStatus.valueOf(in.readUTF());
        backupFilesystem = in.readUTF();
        baseFolder = in.readUTF();
        backupScope = BackupScope.valueOf(in.readUTF());
        incrementalBackup = in.readBoolean();
        incrementalParentBackupID = in.readLong();
        backupTimestamp = in.readLong();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
//  FIXME reenable throw     if(true)
//          throw new UnsupportedOperationException("ENCODE TRANSACTION");
        out.writeUTF(backupStatus.toString());
        out.writeUTF(backupFilesystem);
        out.writeUTF(baseFolder);
        out.writeUTF(backupScope.toString());
        out.writeBoolean(incrementalBackup);
        out.writeLong(incrementalParentBackupID);
        out.writeLong(backupTimestamp);
    }

    // Writes derby properties stored in ZK
    public void createProperties() throws StandardException, IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(getBackupFilesystem()),SpliceConstants.config);
        for (String property : SpliceUtils.listProperties()) {
            byte[] value = SpliceUtils.getProperty(property);

            FSDataOutputStream out = fileSystem.create(new Path(getPropertiesBackupFilesystemAsPath(), property));
            out.writeInt(value.length);
            out.write(value);
            out.close();
        }
    }

    // Write metadata, including timestamp source's last timestamp
    // this has to be called after all tables have been dumped.
    public void createMetadata() throws StandardException, IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(getBackupFilesystem()),SpliceConstants.config);
        byte[] version = Bytes.toBytes(backupVersion);

        FSDataOutputStream out = fileSystem.create(new Path(getMetaBackupFilesystemAsPath(), VERSION_FILE));
        out.writeInt(version.length);
        out.write(version);
        out.close();

        byte[] value = Bytes.toBytes(backupTimestamp);
        out = fileSystem.create(new Path(getMetaBackupFilesystemAsPath(), BACKUP_TIMESTAMP_FILE));
        out.writeInt(value.length);
        out.write(value);
        out.close();

        long timestampSource = TransactionTimestamps.getTimestampSource().nextTimestamp();
        value = Bytes.toBytes(timestampSource);
        out = fileSystem.create(new Path(getMetaBackupFilesystemAsPath(), TIMESTAMP_SOURCE_FILE));
        out.writeInt(value.length);
        out.write(value);
        out.close();

        long conglomerateSequence = ConglomerateUtils.getNextConglomerateId();
        value = Bytes.toBytes(conglomerateSequence);
        out = fileSystem.create(new Path(getMetaBackupFilesystemAsPath(), CONGLOMERATE_SEQUENCE_FILE));
        out.writeInt(value.length);
        out.write(value);
        out.close();
    }


    public void restoreMetadata() throws StandardException, IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(getBackupFilesystem()),SpliceConstants.config);

        FSDataInputStream in = fileSystem.open(new Path(getMetaBackupFilesystemAsPath(), VERSION_FILE));
        int len = in.readInt();
        byte[] value = new byte[len];
        in.readFully(value);
        backupVersion = Bytes.toString(value);
        in.close();

        in = fileSystem.open(new Path(getMetaBackupFilesystemAsPath(), BACKUP_TIMESTAMP_FILE));
        len = in.readInt();
        value = new byte[len];
        in.readFully(value);
        backupTimestamp = Bytes.toLong(value);
        in.close();

        in = fileSystem.open(new Path(getMetaBackupFilesystemAsPath(), TIMESTAMP_SOURCE_FILE));
        len = in.readInt();
        value = new byte[len];
        in.readFully(value);
        timestampSource = Bytes.toLong(value);
        in.close();

        setTimestampSource(timestampSource);

        in = fileSystem.open(new Path(getMetaBackupFilesystemAsPath(), CONGLOMERATE_SEQUENCE_FILE));
        len = in.readInt();
        value = new byte[len];
        in.readFully(value);
        long conglomerateId = Bytes.toLong(value);
        in.close();

        ConglomerateUtils.setNextConglomerateId(conglomerateId);
    }

    // TODO This is hardcoded to the current implementation of Timestamp Source, should be moved to the appropriate class
    private void setTimestampSource(long timestampSource) throws StandardException {
        RecoverableZooKeeper rzk = ZkUtils.getRecoverableZooKeeper();
        String node = SpliceConstants.zkSpliceMaxReservedTimestampPath;
        byte[] data = Bytes.toBytes(timestampSource);
        try {
            rzk.setData(node, data, -1 /* version */);
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    // Restores derby properties to ZK
    public void restoreProperties() throws StandardException, IOException {
        SpliceUtils.clearProperties();
        FileSystem fileSystem = FileSystem.get(URI.create(getBackupFilesystem()),SpliceConstants.config);
        for (FileStatus property : fileSystem.listStatus(getPropertiesBackupFilesystemAsPath())) {
            FSDataInputStream in = fileSystem.open(property.getPath());
            int length = in.readInt();
            byte[] value = new byte[length];
            in.readFully(value, 0, length);

            SpliceUtils.addProperty(property.getPath().getName(), Bytes.toString(value));
        }
    }

    public void moveToBaseFolder() throws IOException {
        if (!temporaryBaseFolder) return;

        FileSystem fileSystem = FileSystem.get(URI.create(getBackupFilesystem()),SpliceConstants.config);
        Path oldBase = new Path(backupFilesystem + "/" + BACKUP_OLD_BASE_FOLDER);
        if (fileSystem.exists(oldBase)) {
            fileSystem.delete(oldBase, true);
        }
        Path base = new Path(backupFilesystem + "/" + BACKUP_BASE_FOLDER);
        fileSystem.rename(base, oldBase);

        fileSystem.rename(getBaseBackupFilesystemAsPath(), base);
    }

    public boolean isTemporaryBaseFolder() {
        return temporaryBaseFolder;
    }

    public void setTemporaryBaseFolder(boolean temporaryBaseFolder) {
        this.temporaryBaseFolder = temporaryBaseFolder;
    }
}