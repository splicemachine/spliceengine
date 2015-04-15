package com.splicemachine.hbase.backup;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.splicemachine.constants.bytes.BytesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.ConglomerateUtils;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.si.impl.TransactionTimestamps;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;
import com.splicemachine.utils.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;


/**
 *
 * Top level Backup Information about the progress of a backup.
 *
 *
 */
public class Backup implements InternalTable {
	public static final String CONF_BANDWIDTH_MB = "splice.backup.bandwidth.mb";
	public static final String CONF_IOTHROTTLE = "splice.backup.throttle.enabled";
	// Do report every XXX bytes copied (with IO throttling enabled)
	public static final int IO_REPORT_SIZE = 4 * 1024 * 1024;
	// IO buffer size for file copy (IO throttling mode)
    public static final int IO_BUFFER_SIZE = 64 * 1024;
    
    private static Logger LOG = Logger.getLogger(Backup.class);

    public static final String DEFAULT_SCHEMA = "SYS";
    public static final String DEFAULT_TABLE = "SYSBACKUP";
    public static final String BACKUP_BASE_FOLDER = "BACKUP";
    public static final String BACKUP_TEMP_BASE_FOLDER = "BACKUP_TEMP";
    public static final String BACKUP_OLD_BASE_FOLDER = "BACKUP_OLD";
    public static final String BACKUP_TABLE_FOLDER = "tables";
    public static final String BACKUP_META_FOLDER = "meta";
    public static final String BACKUP_PROPERTIES_FOLDER = "properties";
    public static final String BACKUP_VERSION = "1";
    public static final String BACKUP_LOG_FILE_NAME = "backupStatus.log";
    public static final long   BACKUP_TPUT_PER_NODE = 30*1024*1024; // modest 30MB per node

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

    public static final String RUNNING_CHECK = "select backup_id from %s.%s where status = ?";
    public static final String QUERY_PARENT_BACKUP_DIRECTORY = "select filesystem from %s.%s where backup_id = ?";

    public static final String VERSION_FILE = "version";
    public static final String BACKUP_TIMESTAMP_FILE = "backupTimestamp";
    public static final String TIMESTAMP_SOURCE_FILE = "timestampSource";
    public static final String CONGLOMERATE_SEQUENCE_FILE = "conglomerateSequence";
    public static final String PARENT_BACKUP_FILE = "parentBackup";

    private Txn backupTransaction;
    private Timestamp beginBackupTimestamp;
    private Timestamp endBackupTimestamp;
    private BackupStatus backupStatus;
    private String backupFilesystem;
    private String baseFolder;
    private BackupScope backupScope;
    private boolean incrementalBackup;
    private long parentBackupId;
    private long backupId;
    private HashMap<String, BackupItem> backupItems;
    private long backupTimestamp;
    private FSDataOutputStream logFileOut;
    private int totalBackuped = 0;
    private long backupStartTime;
    private int actualBackupCount = 0;

    public long getBackupId() {
        return backupId;
    }

    public void setBackupId(long backupId) {
        this.backupId = backupId;
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

    public Backup () {
    }

    public void incrementActualBackupCount() {
        actualBackupCount++;
    }

    public int getActualBackupCount() {
        return actualBackupCount;
    }

    public void setActualBackupCount(int count) {
        actualBackupCount = count;
    }

    public Txn getBackupTransaction() {
        return backupTransaction;
    }


    public void setBackupTransaction(Txn txn){
        this.backupTransaction = txn;
        this.backupId = txn.getTxnId();
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
    public BackupStatus getBackupStatus() {
        return backupStatus;
    }

    public void setBackupStatus(BackupStatus backupStatus) {
        this.backupStatus = backupStatus;
        if( backupStatus == BackupStatus.F){
            if (logFileOut != null) {
                log("Backup FAILED. ");
            }
        } else if( backupStatus == BackupStatus.S){
        	log("Finished with Success. Total time taken for backup was: "+
               formatTime(System.currentTimeMillis() - backupStartTime));
        }
        if( backupStatus != BackupStatus.I){
        	closeLogFile();
        }
    }

    public void updateProgress()
    {
    	totalBackuped++;
    	log(String.format("%d objects of %d objects backed up..", totalBackuped, backupItems.size()));
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
        this.baseFolder = backupFilesystem + "/" + BACKUP_BASE_FOLDER + "_" + backupId;
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

    public Long getParentBackupId() {
        return parentBackupId;
    }

    public void setParentBackupID(Long parentBackupId) {
        this.parentBackupId = parentBackupId;
    }

    /**
     *
     * Create the initial backup object with a new timestamp cut point.
     * Validates the parent timestamp is before the timestamp obtained from the system.
     * @param backupFileSystem Supported File Systems ?
     * @param backupScope BackupScope
     * @param parentBackupID Last Incremental Backup
     * @return
     * @throws SQLException
     */
    public static Backup createBackup(String backupFileSystem,
                                      BackupScope backupScope,
                                      long parentBackupID) throws SQLException {
        try {
            Txn backupTxn = TransactionLifecycle.getLifecycleManager()
                    .beginTransaction()
                    .elevateToWritable("backup".getBytes());

            if (parentBackupID > 0 && parentBackupID >= backupTxn.getTxnId())
                throw new SQLException(String.format("createBackup attempted to create a backup with an incremental " +
                        "parent id timestamp larger than the current timestamp " +
                        "{incrementalParentBackupID=%d,transactionID=%d",parentBackupID,backupTxn.getTxnId()));
            Backup backup = new Backup();
            backup.setBackupTransaction(backupTxn);
            backup.setBeginBackupTimestamp(new Timestamp(System.currentTimeMillis()));
            backup.setBackupScope(backupScope);
            backup.setIncrementalBackup(parentBackupID >= 0);
            backup.setParentBackupID(parentBackupID);
            backup.setBackupStatus(BackupStatus.I);
            backup.setBackupFilesystem(backupFileSystem);
            backup.setBackupTimestamp(backupTxn.getBeginTimestamp());
            backup.setBackupVersion(BACKUP_VERSION);
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
    public void markBackupSuccessful() {
        this.setBackupStatus(BackupStatus.S);
    }

    public boolean createBaseBackupDirectory() throws IOException, URISyntaxException {
        FileSystem fileSystem = FileSystem.get(URI.create(getBackupFilesystem()), SpliceConstants.config);
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
        
        openLogFile();
        
        return true;
    }

    
    public void start() throws IOException{
    	long estBackupTime = estimateBackupTime();
    	this.backupStartTime = System.currentTimeMillis();
    	Date finishTime = new Date(System.currentTimeMillis()+ estBackupTime * 1000);
    	
    	log("Expected time for backup "+formatTime(estBackupTime * 1000)+
    			". Expected finish on "+ finishTime.toString());
    	
    }
    private void openLogFile() throws IOException
    {
    	// We use baseFolder to keep log file in
    	// Should be called after createBaseBackupDirectory
        FileSystem fs = FileSystem.get(URI.create(getBackupFilesystem()), SpliceConstants.config);
        
        Path logFilePath = new Path(getBaseBackupFilesystemAsPath(), BACKUP_LOG_FILE_NAME);
        this.logFileOut = fs.create(logFilePath, true);

    }
    
    /**
     * Close log file
     * @throws IOException
     */
    private void closeLogFile() {
    	try {
            if (this.logFileOut != null) {
                this.logFileOut.close();
            }
		} catch (IOException e) {
			LOG.error("Failed to close log file", e);
		}
    }
    
    /**
     * Estimated backup time
     * @return time (in seconds)
     * @throws IOException
     */
    private long estimateBackupTime() throws IOException{
    	Map<String, BackupItem> itemMap = getBackupItems();
    	Collection<BackupItem> items = itemMap.values();
    	long clusterSize = getClusterSize();
    	long totalDataSize = getTotalDataSize(items);
    	
    	return (totalDataSize)/(clusterSize * BACKUP_TPUT_PER_NODE);
    }
    
    /**
     * Logs a message
     * @param msg
     * @throws IOException
     */
    private void log(String msg) 
    {
    	LOG.info(msg);
    	try{
            if (this.logFileOut != null) {
                this.logFileOut.writeBytes(msg + "\n");
            }
    	} catch (IOException e){
    		// swallow
    	}
    }
    
    /**
     * Format seconds into dd:hh:mm:ss string
     * @param ms
     * @return formatted string
     */
    private String formatTime(long ms)
    {
    	return StringUtils.formatTime(ms);
    }
    
    /**
     * Returns total space consumed by HBase
     * @param items
     * @return
     * @throws IOException
     */
    private long getTotalDataSize(Collection<BackupItem> items) throws IOException {
    	FileSystem fs = FileSystem.get(SpliceConstants.config);
    	Path hbaseRoot = FSUtils.getRootDir(SpliceConstants.config);
    	ContentSummary sum = fs.getContentSummary(hbaseRoot);
    	// TODO: does it work for directories?
    	return sum.getSpaceConsumed();
	}

	private int getClusterSize() throws IOException {
        HBaseAdmin admin = SpliceUtilities.getAdmin();
        return admin.getClusterStatus().getServersSize();
	}

	public void addBackupItem(BackupItem backupItem) {
        if (backupItems == null) {
            backupItems = new HashMap<>();
        }
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

    public void createBackupItems(HBaseAdmin admin, Set<String> snapshotNameSet, Set<String> newSnapshotNameSet)
                                                                            throws IOException, StandardException {
        HTableDescriptor[] descriptorArray = admin.listTables();

        for (HTableDescriptor descriptor: descriptorArray) {
        	// DB-3089
        	if(isTempTable(descriptor) || isTemporaryTable(descriptor.getNameAsString())) continue;
            BackupItem item = new BackupItem(descriptor,this);
            item.createSnapshot(admin, backupTransaction.getBeginTimestamp(), newSnapshotNameSet);
            item.setLastSnapshotName(snapshotNameSet);
            addBackupItem(item);
        }
    }

    private boolean isTempTable(HTableDescriptor descriptor) {
		byte[] tableName = descriptor.getName();
		String strName = new String(tableName);
		if(strName.equals(SpliceConstants.TEMP_TABLE)) {
			LOG.info("Skipping "+strName);
			return true;
		}
    	return false;
	}

    /**
     * Whether a table is a temporary table?
     * @param tableName name of HBase table
     * @return
     */
    private boolean isTemporaryTable(String tableName) {

        Long congId = null;
        try{
            congId = Long.parseLong(tableName);
        }
        catch (NumberFormatException nfe) {
            return false;
        }

        Connection connection = null;
        ResultSet rs = null;
        try {
            String sqlText = "select t.tableid from sys.systables t, sys.sysconglomerates c " +
                    "where t.tableid=c.tableid and c.conglomeratenumber=?";
            connection = SpliceDriver.driver().getInternalConnection();
            PreparedStatement ps = connection.prepareStatement(sqlText);
            ps.setLong(1, congId);
            rs = ps.executeQuery();
            if (rs.next()) {
                String s = rs.getString(1);
                UUID tableId = new BasicUUID(s);
                LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
                DataDictionary dd = lcc.getDataDictionary();
                TableDescriptor td = dd.getTableDescriptor(tableId);
                return !td.isPersistent();
            }
        }
        catch (Exception e) {
            SpliceLogUtils.warn(LOG, "%s", e.getMessage());
        }
        return false;
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
        parentBackupId = in.readLong();
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
        out.writeLong(parentBackupId);
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
    public void createMetadata(long parentBackupId) throws StandardException, IOException {
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

        out = fileSystem.create(new Path(getMetaBackupFilesystemAsPath(), PARENT_BACKUP_FILE));
        out.writeLong(getBackupId());
        out.writeLong(parentBackupId);

        if (parentBackupId > 0) {
            String parentBackupDir = BackupUtils.getBackupDirectory(parentBackupId, backupTransaction);
            out.writeUTF(parentBackupDir);
        }
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
        Path base = new Path(backupFilesystem + "/" + BACKUP_BASE_FOLDER + "_" + backupTransaction.getBeginTimestamp());
        fileSystem.rename(base, oldBase);

        fileSystem.rename(getBaseBackupFilesystemAsPath(), base);
    }

    public boolean isTemporaryBaseFolder() {
        return temporaryBaseFolder;
    }
    

    public void registerBackup() throws KeeperException, InterruptedException, StandardException {
        RecoverableZooKeeper zooKeeper = ZkUtils.getRecoverableZooKeeper();
        try {
            byte[] backupId = BytesUtil.longToBytes(backupTransaction.getTxnId());
            zooKeeper.create(SpliceConstants.DEFAULT_BACKUP_PATH, backupId, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException e) {
            if(e.code()== KeeperException.Code.NODEEXISTS){
                //it's already been created, so nothing to do
                long id = BytesUtil.bytesToLong(zooKeeper.getData(SpliceConstants.DEFAULT_BACKUP_PATH, false, null), 0);
                throw StandardException.newException(String.format("A concurrent backup with id of %d is running.", id));
            }
            else {
                throw e;
            }
        }
    }

    public void deregisterBackup() throws KeeperException, InterruptedException {
        RecoverableZooKeeper zooKeeper = ZkUtils.getRecoverableZooKeeper();
        if (zooKeeper.exists(SpliceConstants.DEFAULT_BACKUP_PATH, false) != null) {
            long id = BytesUtil.bytesToLong(zooKeeper.getData(SpliceConstants.DEFAULT_BACKUP_PATH, false, null), 0);
            if (id == this.backupId) {
                zooKeeper.delete(SpliceConstants.DEFAULT_BACKUP_PATH, -1);
            }
        }
    }
}
