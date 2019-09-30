/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.compactions;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.constants.EnvUtils;
import com.splicemachine.derby.stream.compaction.SparkCompactionFunction;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.data.hbase.coprocessor.TableType;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.Key;
import java.security.KeyException;
import java.util.Collection;
import java.util.List;

/**
 *
 * Splicemachine Compactor code that is common to all hbase versions.
 *
 */
public class SpliceDefaultCompactorBase extends DefaultCompactor {
    static final boolean allowSpark = true;
    static final Logger LOG = Logger.getLogger(SpliceDefaultCompactor.class);
    long smallestReadPoint;
    String conglomId;
    String tableDisplayName;
    String indexDisplayName;
    static String hostName;

    static final String TABLE_DISPLAY_NAME_ATTR = SIConstants.TABLE_DISPLAY_NAME_ATTR;
    static final String INDEX_DISPLAY_NAME_ATTR = SIConstants.INDEX_DISPLAY_NAME_ATTR;

    public SpliceDefaultCompactorBase(final Configuration conf, final Store store) {
        super(conf, store);
        conglomId = this.store.getTableName().getQualifierAsString();
        tableDisplayName = ((HStore)this.store).getHRegion().getTableDesc().getValue(TABLE_DISPLAY_NAME_ATTR);
        indexDisplayName = ((HStore)this.store).getHRegion().getTableDesc().getValue(INDEX_DISPLAY_NAME_ATTR);

        if (LOG.isDebugEnabled()) {
            SpliceLogUtils.debug(LOG, "Initializing compactor: region=%s", ((HStore)this.store).getHRegion());
        }
    }

    public SpliceDefaultCompactorBase(final Configuration conf, final Store store, long smallestReadPoint) {
        this(conf, store);
        this.smallestReadPoint = smallestReadPoint;
    }

    SparkCompactionFunction getCompactionFunction(boolean isMajor, InetSocketAddress[] favoredNodes) {
        return new SparkCompactionFunction(
                smallestReadPoint,
                store.getTableName().getNamespace(),
                store.getTableName().getQualifier(),
                store.getRegionInfo(),
                store.getFamily().getName(),
                isMajor,
                favoredNodes);
    }

    String getCompactionQueue() {
        SConfiguration config = HConfiguration.getConfiguration();
        return config.getOlapServerIsolatedCompaction() ?
               config.getOlapServerIsolatedCompactionQueueName() :
               SIConstants.OLAP_DEFAULT_QUEUE_NAME;
    }

    String getScope(CompactionRequest request) {
        return String.format("%s Compaction: %s",
                getMajorMinorLabel(request),
                getTableInfoLabel(", "));
    }

    String getMajorMinorLabel(CompactionRequest request) {
        return request.isMajor() ? "Major" : "Minor";
    }

    String getJobGroup(CompactionRequest request,String regionLocation) {
        return regionLocation+":"+Long.toString(request.getSelectionTime());
    }

    String getJobDescription(CompactionRequest request) {
        int size = request.getFiles().size();
        String jobDescription = String.format("%s Compaction: %s, %d %s, Total File Size=%s",
                getMajorMinorLabel(request),
                getTableInfoLabel(", "),
                size,
                (size > 1 ? "Files" : "File"),
                FileUtils.byteCountToDisplaySize(request.getSize()));

        if (size == 1 && !request.isMajor()) {
            Collection<StoreFile> files = request.getFiles();
            for (StoreFile file : files) {
                if(file.isReference()) {
                    return String.join(", ",jobDescription, "StoreFile is a Reference");
                }
            }
        }
        return jobDescription;
    }

    String getTableInfoLabel(String delim) {
        StringBuilder sb = new StringBuilder();
        if (indexDisplayName != null) {
            sb.append(String.format("Index=%s", indexDisplayName));
            sb.append(delim);
        } else if (tableDisplayName != null) {
            sb.append(String.format("Table=%s", tableDisplayName));
            sb.append(delim);
        }
        sb.append(String.format("Conglomerate=%s", conglomId));
        sb.append(delim);
        sb.append(String.format("RegionName=%s", this.store.getRegionInfo().getRegionNameAsString()));
        sb.append(delim);
        sb.append(String.format("RegionId=%d", this.store.getRegionInfo().getRegionId()));
        return sb.toString();
    }

    String jobDetails = null;
    String getJobDetails(CompactionRequest request) {
        if (jobDetails == null) {
            String delim=",\n";
            jobDetails =getTableInfoLabel(delim) +delim
                    +String.format("Region Name=%s",this.store.getRegionInfo().getRegionNameAsString()) +delim
                    +String.format("Region Id=%d",this.store.getRegionInfo().getRegionId()) +delim
                    +String.format("File Count=%d",request.getFiles().size()) +delim
                    +String.format("Total File Size=%s",FileUtils.byteCountToDisplaySize(request.getSize())) +delim
                    +String.format("Type=%s",getMajorMinorLabel(request));
        }
        return jobDetails;
    }

    String getPoolName() {
        return "compaction";
    }

    boolean needsSI(TableName tableName) {
        TableType type = EnvUtils.getTableType(HConfiguration.getConfiguration(), tableName);
        switch (type) {
            case TRANSACTION_TABLE:
            case ROOT_TABLE:
            case META_TABLE:
            case HBASE_TABLE:
                return false;
            case DERBY_SYS_TABLE:
            case USER_TABLE:
                return true;
            default:
                throw new RuntimeException("Unknow table type " + type);
        }
    }

    @Override
    public List<Path> compactForTesting(Collection<StoreFile> filesToCompact, boolean isMajor) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"compactForTesting");
        return super.compactForTesting(filesToCompact, isMajor);
    }

    @Override
    public CompactionProgress getProgress() {
        return super.getProgress();
    }

    @Override
    protected FileDetails getFileDetails(Collection<StoreFile> filesToCompact, boolean allFiles) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getFileDetails");
        return super.getFileDetails(filesToCompact, allFiles);
    }

    @Override
    protected long getSmallestReadPoint() {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getSmallestReadPoint");
        return this.smallestReadPoint;
    }

    @Override
    protected InternalScanner preCreateCoprocScanner(CompactionRequest request, ScanType scanType, long earliestPutTs, List<StoreFileScanner> scanners) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"preCreateCoprocScanner");
        return super.preCreateCoprocScanner(request, scanType, earliestPutTs, scanners);
    }

    @Override
    protected InternalScanner createScanner(Store store, List<StoreFileScanner> scanners, ScanType scanType, long smallestReadPoint, long earliestPutTs) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"createScanner");
        Scan scan = new Scan();
        scan.setMaxVersions(store.getFamily().getMaxVersions());
        return new StoreScanner(store, store.getScanInfo(), scan, scanners,
                scanType, smallestReadPoint, earliestPutTs);
    }

    @Override
    protected InternalScanner createScanner(Store store, List<StoreFileScanner> scanners, long smallestReadPoint, long earliestPutTs, byte[] dropDeletesFromRow, byte[] dropDeletesToRow) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "createScanner");
        return super.createScanner(store, scanners, smallestReadPoint, earliestPutTs, dropDeletesFromRow, dropDeletesToRow);
    }

    /**
     *
     * This is borrowed from DefaultCompactor.
     *
     * @param compression
     * @param includeMVCCReadpoint
     * @param includesTag
     * @param cryptoContext
     * @return
     */
    HFileContext createFileContext(Compression.Algorithm compression,
                                           boolean includeMVCCReadpoint, boolean includesTag, Encryption.Context cryptoContext) {
        if (compression == null) {
            compression = HFile.DEFAULT_COMPRESSION_ALGORITHM;
        }
        HFileContext hFileContext = new HFileContextBuilder()
                .withIncludesMvcc(includeMVCCReadpoint)
                .withIncludesTags(includesTag)
                .withCompression(compression)
                .withCompressTags(store.getFamily().isCompressTags())
                .withChecksumType(HStore.getChecksumType(conf))
                .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf))
                .withBlockSize(store.getFamily().getBlocksize())
                .withHBaseCheckSum(true)
                .withDataBlockEncoding(store.getFamily().getDataBlockEncoding())
                .withEncryptionContext(cryptoContext)
                .withCreateTime(EnvironmentEdgeManager.currentTime())
                .build();
        return hFileContext;
    }

    /**
     *
     * This only overwrites favored nodes when there are none supplied.  I believe in later versions the favoredNodes are
     * populated for region groups.  When this happens, we will pass those favored nodes along.  Until then, we attempt to put the local
     * node in the favored nodes since sometimes Spark Tasks will run compactions remotely.
     *
     * @return
     * @throws IOException
     */
    protected InetSocketAddress[] getFavoredNodes() throws IOException {
        try {
            RegionServerServices rsServices = (RegionServerServices) FieldUtils.readField(((HStore) store).getHRegion(), "rsServices", true);
            InetSocketAddress[] returnAddresses = (InetSocketAddress[]) MethodUtils.invokeMethod(rsServices,"getFavoredNodesForRegion",store.getRegionInfo().getEncodedName());
            if ( (returnAddresses == null || returnAddresses.length == 0)
                    && store.getFileSystem() instanceof HFileSystem
                    && ((HFileSystem)store.getFileSystem()).getBackingFs() instanceof DistributedFileSystem) {
                String[] txvr = conf.get("dfs.datanode.address").split(":"); // hack
                if (txvr.length == 2) {
                    returnAddresses = new InetSocketAddress[1];
                    returnAddresses[0] = new InetSocketAddress(hostName, Integer.parseInt(txvr[1]));
                }
                else {
                    SpliceLogUtils.warn(LOG,"dfs.datanode.address is expected to have form hostname:port but is %s",txvr);
                }
            }
            return returnAddresses;
        } catch (Exception e) {
            SpliceLogUtils.error(LOG,e);
            throw new IOException(e);
        }

    }

    /**
     *
     * Retrieve the Crypto Context.  This is borrowed from the DefaultCompactor logic.
     *
     * @return
     * @throws IOException
     */
    public Encryption.Context getCryptoContext() throws IOException {
        // Crypto context for new store files
        String cipherName = store.getFamily().getEncryptionType();
        if (cipherName != null) {
            Cipher cipher;
            Key key;
            byte[] keyBytes = store.getFamily().getEncryptionKey();
            if (keyBytes != null) {
                // Family provides specific key material
                String masterKeyName = conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY,
                        User.getCurrent().getShortName());
                try {
                    // First try the master key
                    key = EncryptionUtil.unwrapKey(conf, masterKeyName, keyBytes);
                } catch (KeyException e) {
                    // If the current master key fails to unwrap, try the alternate, if
                    // one is configured
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Unable to unwrap key with current master key '" + masterKeyName + "'");
                    }
                    String alternateKeyName =
                            conf.get(HConstants.CRYPTO_MASTERKEY_ALTERNATE_NAME_CONF_KEY);
                    if (alternateKeyName != null) {
                        try {
                            key = EncryptionUtil.unwrapKey(conf, alternateKeyName, keyBytes);
                        } catch (KeyException ex) {
                            throw new IOException(ex);
                        }
                    } else {
                        throw new IOException(e);
                    }
                }
                // Use the algorithm the key wants
                cipher = Encryption.getCipher(conf, key.getAlgorithm());
                if (cipher == null) {
                    throw new RuntimeException("Cipher '" + key.getAlgorithm() + "' is not available");
                }
                // Fail if misconfigured
                // We use the encryption type specified in the column schema as a sanity check on
                // what the wrapped key is telling us
                if (!cipher.getName().equalsIgnoreCase(cipherName)) {
                    throw new RuntimeException("Encryption for family '" + store.getFamily().getNameAsString() +
                            "' configured with type '" + cipherName +
                            "' but key specifies algorithm '" + cipher.getName() + "'");
                }
            } else {
                // Family does not provide key material, create a random key
                cipher = Encryption.getCipher(conf, cipherName);
                if (cipher == null) {
                    throw new RuntimeException("Cipher '" + cipherName + "' is not available");
                }
                key = cipher.getRandomKey();
            }
            Encryption.Context cryptoContext = Encryption.newContext(conf);
            cryptoContext.setCipher(cipher);
            cryptoContext.setKey(key);
            return cryptoContext;
        } else
            return Encryption.Context.NONE;
    }
}
