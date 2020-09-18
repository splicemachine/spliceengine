/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.clearspring.analytics.util.Lists;
import com.splicemachine.EngineDriver;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.constants.EnvUtils;
import com.splicemachine.derby.stream.compaction.SparkCompactionFunction;
import com.splicemachine.hbase.SICompactionScanner;
import com.splicemachine.hbase.SpliceCompactionUtils;
import com.splicemachine.hbase.TransactionsWatcher;
import com.splicemachine.olap.DistributedCompaction;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.data.hbase.coprocessor.TableType;
import com.splicemachine.si.impl.Transaction;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.server.*;
import com.splicemachine.si.impl.server.CompactionContext;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.*;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.security.Key;
import java.security.KeyException;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.hadoop.hbase.regionserver.HStoreFile.EARLIEST_PUT_TS;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.TIMERANGE_KEY;
import static org.apache.hadoop.hbase.regionserver.ScanType.COMPACT_DROP_DELETES;
import static org.apache.hadoop.hbase.regionserver.ScanType.COMPACT_RETAIN_DELETES;

/**
 *
 * Splicemachine Compactor with added code for locality.
 *
 */
public class SpliceDefaultCompactor extends DefaultCompactor {
    private static final boolean allowSpark = true;
    private static final Logger LOG = Logger.getLogger(SpliceDefaultCompactor.class);
    private long smallestReadPoint;
    private String conglomId;
    private String tableDisplayName;
    private String indexDisplayName;
    private static String hostName;
    private boolean isSpark;

    private static final String TABLE_DISPLAY_NAME_ATTR = SIConstants.TABLE_DISPLAY_NAME_ATTR;
    private static final String INDEX_DISPLAY_NAME_ATTR = SIConstants.INDEX_DISPLAY_NAME_ATTR;

    public SpliceDefaultCompactor(final Configuration conf, final HStore store) {
        super(conf, store);
        conglomId = this.store.getTableName().getQualifierAsString();
        tableDisplayName = this.store.getHRegion().getTableDescriptor().getValue(TABLE_DISPLAY_NAME_ATTR);
        indexDisplayName = this.store.getHRegion().getTableDescriptor().getValue(INDEX_DISPLAY_NAME_ATTR);
        isSpark = false;

        if (LOG.isDebugEnabled()) {
            SpliceLogUtils.debug(LOG, "Initializing compactor: region=%s", this.store.getHRegion());
        }
    }

    public SpliceDefaultCompactor(final Configuration conf, final Store store) {
        this(conf, (HStore)store);
    }

    public SpliceDefaultCompactor(final Configuration conf, final Store store, long smallestReadPoint) {
        this(conf, store);
        this.smallestReadPoint = smallestReadPoint;
    }

    @Override
    @SuppressFBWarnings(value="ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", justification="static attribute hostname is set from here")
    public List<Path> compact(CompactionRequestImpl request, ThroughputController throughputController, User user) throws IOException {
        assert request instanceof SpliceCompactionRequest;
        SpliceCompactionRequest spliceRequest = (SpliceCompactionRequest)request;
        // Used if we cannot compact in Spark
        spliceRequest.setPurgeConfig(buildPurgeConfig(request, TransactionsWatcher.getLowWatermarkTransaction()));

        if(!allowSpark || store.getRegionInfo().getTable().isSystemTable()) {
            isSpark = false;
            return super.compact(request, throughputController, user);
        }

        SpliceLogUtils.trace(LOG, "compact(): request=%s", request);
        smallestReadPoint = store.getSmallestReadPoint();
        FileDetails fd = getFileDetails(request.getFiles(), request.isAllFiles());
        this.progress = new CompactionProgress(fd.maxKeyCount);
        List<String> files = new ArrayList<>();
        for (StoreFile sf : request.getFiles()) {
            files.add(sf.getPath().toString());
        }

        ScanType scanType = request.isAllFiles() ? COMPACT_DROP_DELETES : COMPACT_RETAIN_DELETES;
        // trigger MemstoreAwareObserver
        postCreateCoprocScanner(request, scanType, null,user);

        if (hostName == null)
            hostName = RSRpcServices.getHostname(conf,false);

        SConfiguration config = HConfiguration.getConfiguration();
        DistributedCompaction jobRequest=new DistributedCompaction(
                getCompactionFunction(request.isMajor(),getFavoredNodes()),
                files,
                getJobDetails(request),
                getJobGroup(request,hostName),
                getJobDescription(request),
                getPoolName(),
                getScope(request),
                hostName,
                config.getOlapCompactionMaximumWait());
        CompactionResult result = null;

        EngineDriver driver = EngineDriver.driver();
        if (driver == null) {
            throw new IOException("EngineDriver not available, compaction aborted");
        }
        Future<CompactionResult> futureResult = driver.getOlapClient().submit(jobRequest, getCompactionQueue());
        while (result == null) {
            try {
                result = futureResult.get(config.getOlapClientTickTime(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                //we were interrupted processing, so we're shutting down. Nothing to be done, just die gracefully
                Thread.currentThread().interrupt();
                throw new IOException(e);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof RejectedExecutionException) {
                    LOG.warn("Spark compaction execution rejected, falling back to RegionServer execution", e.getCause());
                    return super.compact(request, throughputController, user);
                }
                throw Exceptions.rawIOException(e.getCause());
            } catch (TimeoutException e) {
                // check region write status
                if (!store.areWritesEnabled()) {
                    futureResult.cancel(true);
                    progress.cancel();
                    // TODO should we cleanup files written by Spark?
                    throw new IOException("Region has been closed, compaction aborted");
                }
            }
        }

        List<String> sPaths = result.getPaths();

        SpliceLogUtils.trace(LOG, "Paths Returned: %s", sPaths);

        this.progress.complete();

        spliceRequest.preStorefilesRename();

        List<Path> paths = new ArrayList<>();
        for (String spath : sPaths) {
            paths.add(new Path(spath));
        }

        return paths;
    }

    private SparkCompactionFunction getCompactionFunction(boolean isMajor, InetSocketAddress[] favoredNodes) {
        return new SparkCompactionFunction(
                smallestReadPoint,
                store.getTableName().getNamespace(),
                store.getTableName().getQualifier(),
                store.getRegionInfo(),
                store.getColumnFamilyDescriptor().getName(),
                isMajor,
                TransactionsWatcher.getLowWatermarkTransaction(),
                favoredNodes);
    }

    private String getCompactionQueue() {
        SConfiguration config = HConfiguration.getConfiguration();
        return config.getOlapServerIsolatedCompaction() ?
                config.getOlapServerIsolatedCompactionQueueName() :
                SIConstants.OLAP_DEFAULT_QUEUE_NAME;
    }

    private String getScope(CompactionRequest request) {
        return String.format("%s Compaction: %s",
                getMajorMinorLabel(request),
                getTableInfoLabel(", "));
    }

    private String getJobDescription(CompactionRequest request) {
        int size = request.getFiles().size();
        String jobDescription = String.format("%s Compaction: %s, %d %s, Total File Size=%s",
                getMajorMinorLabel(request),
                getTableInfoLabel(", "),
                size,
                (size > 1 ? "Files" : "File"),
                FileUtils.byteCountToDisplaySize(request.getSize()));

        if (size == 1 && !request.isMajor()) {
            Collection<? extends StoreFile> files = request.getFiles();
            for (StoreFile file : files) {
                if(file.isReference()) {
                    return String.join(", ",jobDescription, "StoreFile is a Reference");
                }
            }
        }
        return jobDescription;
    }

    private String getMajorMinorLabel(CompactionRequest request) {
        return request.isMajor() ? "Major" : "Minor";
    }

    private String getTableInfoLabel(String delim) {
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
        sb.append(String.format("Region Encoded Name=%s, RegionId=%d",this.store.getRegionInfo().getEncodedName(), this.store.getRegionInfo().getRegionId()));
        return sb.toString();
    }

    private String getJobGroup(CompactionRequest request,String regionLocation) {
        return regionLocation+":"+Long.toString(request.getSelectionTime());
    }

    private String jobDetails = null;
    private String getJobDetails(CompactionRequest request) {
        if (jobDetails == null) {
            String delim=",\n";
            jobDetails =getTableInfoLabel(delim) +delim
                    +String.format("File Count=%d",request.getFiles().size()) +delim
                    +String.format("Total File Size=%s",FileUtils.byteCountToDisplaySize(request.getSize())) +delim
                    +String.format("Type=%s",getMajorMinorLabel(request));
        }
        return jobDetails;
    }

    private String getPoolName() {
        return "compaction";
    }

    public List<Path> sparkCompact(CompactionRequest request, long transactionLowWatermark,
                                   CompactionContext context, InetSocketAddress[] favoredNodes) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "sparkCompact(): CompactionRequest=%s", request);

        FileDetails fd = getFileDetails(((CompactionRequestImpl)request).getFiles(), request.isAllFiles());
        this.progress = new CompactionProgress(fd.maxKeyCount);

        // Find the smallest read point across all the Scanners.
        long smallestReadPoint = getSmallestReadPoint();

        List<StoreFileScanner> scanners;
        Collection<HStoreFile> readersToClose;
        // Tell HDFS it can drop data out of the caches after reading them, we are compacting on Spark and won't need
        // that data anytime soon
        final boolean dropBehind = true;
        if (this.conf.getBoolean("hbase.regionserver.compaction.private.readers", false)) {
            // clone all StoreFiles, so we'll do the compaction on a independent copy of StoreFiles,
            // HFileFiles, and their readers
            readersToClose =new ArrayList<>(request.getFiles().size());
            for (StoreFile f : request.getFiles()) {
                readersToClose.add((HStoreFile) f);
            }
            scanners = createFileScanners(readersToClose, smallestReadPoint, dropBehind);
        } else {
            readersToClose = Collections.emptyList();
            scanners = createFileScanners(((CompactionRequestImpl)request).getFiles(), smallestReadPoint, dropBehind);
        }

        StoreFileWriter writer = null;
        List<Path> newFiles =new ArrayList<>();
        boolean cleanSeqId = false;
        IOException e = null;
        try {
            InternalScanner scanner = null;
            try {
                /* Include deletes, unless we are doing a compaction of all files */
                ScanType scanType = request.isAllFiles() ? COMPACT_DROP_DELETES : COMPACT_RETAIN_DELETES;
                ScanInfo scanInfo = preCreateCoprocScanner(request, scanType, fd.earliestPutTs, scanners, null);
                scanner = createScanner(store, scanners, scanType, smallestReadPoint, fd.earliestPutTs);
                if (needsSI(store.getTableName())) {
                    SIDriver driver=SIDriver.driver();
                    SConfiguration config = HConfiguration.getConfiguration();
                    double resolutionShare = config.getOlapCompactionResolutionShare();
                    int bufferSize = config.getOlapCompactionResolutionBufferSize();
                    boolean blocking = config.getOlapCompactionBlocking();
                    SICompactionState state = new SICompactionState(driver.getTxnSupplier(),
                            driver.getConfiguration().getActiveTransactionMaxCacheSize(), context, blocking ? driver.getExecutorService() : driver.getRejectingExecutorService());

                    SICompactionScanner siScanner = new SICompactionScanner(
                            state, scanner, buildPurgeConfig(request, transactionLowWatermark), resolutionShare, bufferSize, context);
                    siScanner.start();
                    scanner = siScanner;
                }
                // Create the writer even if no kv(Empty store file is also ok),
                // because we need record the max seq id for the store file, see HBASE-6059
                if(fd.minSeqIdToKeep > 0) {
                    smallestReadPoint = Math.min(fd.minSeqIdToKeep, smallestReadPoint);
                    cleanSeqId = true;
                }

                writer = createTmpWriter(fd, dropBehind,favoredNodes);
                isSpark = true;
                boolean finished = performCompaction(fd, scanner,  writer, smallestReadPoint, cleanSeqId,
                        NoLimitThroughputController.INSTANCE, request.isMajor(), request.getFiles().size());
                if (!finished) {
                    writer.close();
                    store.getFileSystem().delete(writer.getPath(), false);
                    writer = null;
                    throw new InterruptedIOException( "Aborting compaction of store " + store +
                            " in region " + store.getRegionInfo().getRegionNameAsString() +
                            " because it was interrupted.");
                }
            } finally {
                if (scanner != null) {
                    scanner.close();
                }
            }
        } catch (IOException ioe) {
            e = ioe;
            throw ioe;
        }
        finally {
            try {
                if (writer != null) {
                    if (e != null) {
                        writer.close();
                    } else {
                        writer.appendMetadata(fd.maxSeqId, request.isAllFiles());
                        writer.close();
                        newFiles.add(writer.getPath());
                    }
                }
            } finally {
                for (HStoreFile f : readersToClose) {
                    try {
                        f.closeStoreFile(true);
                    } catch (IOException ioe) {
                        LOG.warn("Exception closing " + f, ioe);
                    }
                }
            }
        }
        return newFiles;
    }

    private boolean needsSI(TableName tableName) {
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
    public CompactionProgress getProgress() {
        return super.getProgress();
    }


    private long getSmallestReadPoint() {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getSmallestReadPoint");
        return this.smallestReadPoint;
    }

    @Override
    protected boolean performCompaction(Compactor.FileDetails fd, InternalScanner scanner, CellSink writer,
                                        long smallestReadPoint, boolean cleanSeqId,
                                        ThroughputController throughputController, boolean major,
                                        int numofFilesToCompact) throws IOException {
        SpliceLogUtils.trace(LOG, "performCompaction");
        long bytesWritten = 0;
        final long closeCheckInterval = HStore.getCloseCheckInterval();
        // these variables are used to log the progress of long-running compactions
        long bytesWrittenProgress = 0, lastMillis = 0, now = 0;
        if (LOG.isDebugEnabled()) {
            lastMillis = EnvironmentEdgeManager.currentTime();
        }
        // Since scanner.next() can return 'false' but still be delivering data,
        // we have to use a do/while loop.
        List<Cell> cells = new ArrayList<>();
        boolean hasMore;
        ScannerContext scannerContext = ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();
        do {
            hasMore = scanner.next(cells, scannerContext);
            if (LOG.isDebugEnabled()) {
                now = EnvironmentEdgeManager.currentTime();
            }
            // output to writer:
            for (Cell c : cells) {
                if (cleanSeqId && c.getSequenceId() <= smallestReadPoint) {
                    CellUtil.setSequenceId(c, 0);
                }
                writer.append(c);
                long len = 0;
                // AbstractSICompactionScanner could eliminate the cells completely during compaction making the output
                // byte size potentially zero. Therefore, we query the size of the input cells before compaction directly
                // from it before compaction (DB-9554)
                if (scanner instanceof AbstractSICompactionScanner) {
                    len = ((AbstractSICompactionScanner) scanner).numberOfScannedBytes();
                } else {
                    len = KeyValueUtil.length(c);
                }
                ++progress.currentCompactedKVs;
                progress.totalCompactedSize += len;
                if (LOG.isDebugEnabled()) {
                    bytesWrittenProgress += len;
                }
                // check periodically to see if a system stop is requested
                if (closeCheckInterval > 0) {
                    bytesWritten += len;
                    if (bytesWritten > closeCheckInterval) {
                        bytesWritten = 0; // reset so check whether cancellation is requested in the next <closeCheckInterval>-bytes mark
                        if ((isSpark && TaskContext.get().isInterrupted()) || (!isSpark && !store.areWritesEnabled())) {
                            SpliceLogUtils.debug(LOG, "Compaction cancelled");
                            progress.cancel();
                            return false;
                        }
                    }
                }
            }
            // Log the progress of long-running compactions every minute if logging at DEBUG level
            if (LOG.isDebugEnabled()) {
                if ((now - lastMillis) >= 60 * 1000) {
                    LOG.debug(String.format("Compaction progress: %s, rate=%.2f kB/sec", progress,
                            (bytesWrittenProgress / 1024.0) / ((now - lastMillis) / 1000.0)));
                    lastMillis = now;
                    bytesWrittenProgress = 0;
                }
            }
            cells.clear();
        } while (hasMore);
        progress.complete();
        return true;
    }

    protected InternalScanner createScanner(HStore store, List<StoreFileScanner> scanners, ScanType scanType, long smallestReadPoint, long earliestPutTs) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"createScanner");
        Scan scan = new Scan();
        scan.setMaxVersions(store.getColumnFamilyDescriptor().getMaxVersions());
        return new StoreScanner(store, store.getScanInfo(), scanners, scanType, smallestReadPoint, earliestPutTs);
    }

    /**
     *
     * createWriterInTmp borrowed from DefaultCompactor to fix scope issues.
     *
     * @param maxKeyCount
     * @param compression
     * @param isCompaction
     * @param includeMVCCReadpoint
     * @param includesTag
     * @param shouldDropBehind
     * @param favoredNodes
     * @return
     * @throws IOException
     */
    public StoreFileWriter createWriterInTmp(long maxKeyCount, Compression.Algorithm compression,
                                              boolean isCompaction, boolean includeMVCCReadpoint, boolean includesTag,
                                              boolean shouldDropBehind, InetSocketAddress[] favoredNodes)
            throws IOException {
        final CacheConfig writerCacheConf;
        if (LOG.isDebugEnabled()) {
            SpliceLogUtils.debug(LOG,"createWriterInTmp with favoredNodes=%s",favoredNodes==null?"null": Arrays.toString(favoredNodes));

        }
        if (isCompaction) {
            // Don't cache data on write on compactions.
            writerCacheConf = new CacheConfig(store.getCacheConfig());
            writerCacheConf.setCacheDataOnWrite(false);
        } else {
            writerCacheConf = store.getCacheConfig();
        }
        // Required for Hbase Writer to pass on Favored Nodes
        HFileSystem wrappedFileSystem = new HFileSystem(store.getFileSystem());

        HFileContext hFileContext = createFileContext(compression, includeMVCCReadpoint, includesTag,
                getCryptoContext());
        StoreFileWriter w = new StoreFileWriter.Builder(conf, writerCacheConf,
                wrappedFileSystem)
                .withFilePath( ((HStore)store).getRegionFileSystem().createTempName())
                .withComparator(store.getComparator())
                .withBloomType(store.getColumnFamilyDescriptor().getBloomFilterType())
                .withMaxKeyCount(maxKeyCount)
                .withFavoredNodes(favoredNodes)
                .withFileContext(hFileContext)
                .withShouldDropCacheBehind(shouldDropBehind)
                .build();
        return w;
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
    private HFileContext createFileContext(Compression.Algorithm compression,
                                           boolean includeMVCCReadpoint, boolean includesTag, Encryption.Context cryptoContext) {
        if (compression == null) {
            compression = HFile.DEFAULT_COMPRESSION_ALGORITHM;
        }
        HFileContext hFileContext = new HFileContextBuilder()
                .withIncludesMvcc(includeMVCCReadpoint)
                .withIncludesTags(includesTag)
                .withCompression(compression)
                .withCompressTags(store.getColumnFamilyDescriptor().isCompressTags())
                .withChecksumType(HStore.getChecksumType(conf))
                .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf))
                .withBlockSize(store.getColumnFamilyDescriptor().getBlocksize())
                .withHBaseCheckSum(true)
                .withDataBlockEncoding(store.getColumnFamilyDescriptor().getDataBlockEncoding())
                .withEncryptionContext(cryptoContext)
                .withCreateTime(EnvironmentEdgeManager.currentTime())
                .build();
        return hFileContext;
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
        String cipherName = store.getColumnFamilyDescriptor().getEncryptionType();
        if (cipherName != null) {
            Cipher cipher;
            Key key;
            byte[] keyBytes = store.getColumnFamilyDescriptor().getEncryptionKey();
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
                    throw new RuntimeException("Encryption for family '" + store.getColumnFamilyDescriptor().getNameAsString() +
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

    /**
     * Creates a writer for a new file in a temporary directory.  This is pulled forward from DefaultCompactor
     * to handle some scoping issues.
     *
     * @param fd The file details.
     * @return Writer for a new StoreFile in the tmp dir.
     * @throws IOException
     */
    protected StoreFileWriter createTmpWriter(FileDetails fd, boolean shouldDropBehind, InetSocketAddress[] favoredNodes)
            throws IOException {

        // When all MVCC readpoints are 0, don't write them.
        // See HBASE-8166, HBASE-12600, and HBASE-13389.

        return createWriterInTmp(fd.maxKeyCount, this.compactionCompression,
            /* isCompaction = */ true,
            /* includeMVCCReadpoint = */ fd.maxMVCCReadpoint > 0,
            /* includesTags = */ fd.maxTagsLength > 0,
            /* shouldDropBehind = */ shouldDropBehind,
                favoredNodes);
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

    public List<StoreFileScanner> createFileScanners(
            final Collection<StoreFile> filesToCompact,
            long smallestReadPoint) throws IOException {
        List<HStoreFile> hStoreFileList = Lists.newArrayList();
        for (StoreFile storeFile : filesToCompact) {
            hStoreFileList.add((HStoreFile) storeFile);
        }
        return StoreFileScanner.getScannersForStoreFiles(hStoreFileList,
        /* cache blocks = */ false,
        /* use pread = */ false,
        /* is compaction */ true,
        /* use Drop Behind */ false,
                smallestReadPoint);
    }

    protected InternalScanner postCreateCoprocScanner(final CompactionRequest request,
                                                      final ScanType scanType, final InternalScanner scanner, User user) throws IOException {
        if (store.getCoprocessorHost() == null) return scanner;
        if (user == null) {
            return store.getCoprocessorHost().preCompact(store, scanner, scanType, null, request, null);
        } else {
            try {
                return user.getUGI().doAs(new PrivilegedExceptionAction<InternalScanner>() {
                    @Override
                    public InternalScanner run() throws Exception {
                        return store.getCoprocessorHost().preCompact(store, scanner, scanType, null, request, null);
                    }
                });
            } catch (InterruptedException ie) {
                InterruptedIOException iioe = new InterruptedIOException();
                iioe.initCause(ie);
                throw iioe;
            }
        }
    }

    protected static class SpliceFileDetails extends Compactor.FileDetails{
        public SpliceFileDetails() {}
    }

    /**
     * Extracts some details about the files to compact that are commonly needed by compactors.
     * @param filesToCompact Files.
     * @param allFiles Whether all files are included for compaction
     * @return The result.
     */
    private Compactor.FileDetails getFileDetails(
            Collection<HStoreFile> filesToCompact, boolean allFiles) throws IOException {
        FileDetails fd = new SpliceFileDetails();
        long oldestHFileTimestampToKeepMVCC = System.currentTimeMillis() -
                (1000L * 60 * 60 * 24 * this.keepSeqIdPeriod);

        for (HStoreFile file : filesToCompact) {
            if(allFiles && (file.getModificationTimestamp() < oldestHFileTimestampToKeepMVCC)) {
                // when isAllFiles is true, all files are compacted so we can calculate the smallest
                // MVCC value to keep
                if(fd.minSeqIdToKeep < file.getMaxMemStoreTS()) {
                    fd.minSeqIdToKeep = file.getMaxMemStoreTS();
                }
            }
            long seqNum = file.getMaxSequenceId();
            fd.maxSeqId = Math.max(fd.maxSeqId, seqNum);
            StoreFileReader r = file.getReader();
            if (r == null) {
                LOG.warn("Null reader for " + file.getPath());
                continue;
            }
            // NOTE: use getEntries when compacting instead of getFilterEntries, otherwise under-sized
            // blooms can cause progress to be miscalculated or if the user switches bloom
            // type (e.g. from ROW to ROWCOL)
            long keyCount = r.getEntries();
            fd.maxKeyCount += keyCount;
            // calculate the latest MVCC readpoint in any of the involved store files
            Map<byte[], byte[]> fileInfo = r.loadFileInfo();
            byte[] tmp = null;
            // Get and set the real MVCCReadpoint for bulk loaded files, which is the
            // SeqId number.
            if (r.isBulkLoaded()) {
                fd.maxMVCCReadpoint = Math.max(fd.maxMVCCReadpoint, r.getSequenceID());
            }
            else {
                tmp = fileInfo.get(HFile.Writer.MAX_MEMSTORE_TS_KEY);
                if (tmp != null) {
                    fd.maxMVCCReadpoint = Math.max(fd.maxMVCCReadpoint, Bytes.toLong(tmp));
                }
            }
            tmp = fileInfo.get(HFile.FileInfo.MAX_TAGS_LEN);
            if (tmp != null) {
                fd.maxTagsLength = Math.max(fd.maxTagsLength, Bytes.toInt(tmp));
            }
            // If required, calculate the earliest put timestamp of all involved storefiles.
            // This is used to remove family delete marker during compaction.
            long earliestPutTs = 0;
            if (allFiles) {
                tmp = fileInfo.get(EARLIEST_PUT_TS);
                if (tmp == null) {
                    // There's a file with no information, must be an old one
                    // assume we have very old puts
                    fd.earliestPutTs = earliestPutTs = HConstants.OLDEST_TIMESTAMP;
                } else {
                    earliestPutTs = Bytes.toLong(tmp);
                    fd.earliestPutTs = Math.min(fd.earliestPutTs, earliestPutTs);
                }
            }
            tmp = fileInfo.get(TIMERANGE_KEY);
            fd.latestPutTs = tmp == null ? HConstants.LATEST_TIMESTAMP: TimeRangeTracker.parseFrom(tmp).getMax();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Compacting " + file +
                        ", keycount=" + keyCount +
                        ", bloomtype=" + r.getBloomFilterType().toString() +
                        ", size=" + StringUtils.TraditionalBinaryPrefix.long2String(r.length(), "", 1) +
                        ", encoding=" + r.getHFileReader().getDataBlockEncoding() +
                        ", seqNum=" + seqNum +
                        (allFiles ? ", earliestPutTs=" + earliestPutTs: ""));
            }
        }
        return fd;
    }

    private List<StoreFileScanner> createFileScanners(Collection<HStoreFile> filesToCompact, long smallestReadPoint, boolean useDropBehind) throws IOException {
        for (HStoreFile file:filesToCompact) {
            file.initReader(); // a hack to init metadata map, so that HStoreFile.getBulkLoadTimestamp does not fail.
        }
        return StoreFileScanner.getScannersForCompaction(filesToCompact, useDropBehind, smallestReadPoint);
    }


    protected ScanInfo preCreateCoprocScanner(final CompactionRequest request,
                                                     final ScanType scanType,
                                                     final long earliestPutTs,
                                                     final List<StoreFileScanner> scanners,
                                                     User user) throws IOException {
        if (store.getCoprocessorHost() == null) return null;
        if (user == null) {
            return store.getCoprocessorHost().preCompactScannerOpen(store, scanType, null, request, null);
        } else {
            try {
                return user.getUGI().doAs(new PrivilegedExceptionAction<ScanInfo>() {
                    @Override
                    public ScanInfo run() throws Exception {
                        return store.getCoprocessorHost().preCompactScannerOpen(store, scanType, null, request, null);
                    }
                });
            } catch (InterruptedException ie) {
                InterruptedIOException iioe = new InterruptedIOException();
                iioe.initCause(ie);
                throw iioe;
            }
        }
    }

    private PurgeConfig buildPurgeConfig(CompactionRequest request, long transactionLowWatermark) throws IOException {
        SConfiguration config = HConfiguration.getConfiguration();
        PurgeConfigBuilder purgeConfig = new PurgeConfigBuilder();
        if (SpliceCompactionUtils.forcePurgeDeletes(store) && request.isMajor()) {
            purgeConfig.forcePurgeDeletes();
        } else if (config.getOlapCompactionAutomaticallyPurgeDeletedRows()) {
            purgeConfig.purgeDeletesDuringCompaction(request.isMajor());
        } else {
            purgeConfig.noPurgeDeletes();
        }
        purgeConfig.purgeUpdates(config.getOlapCompactionAutomaticallyPurgeOldUpdates());
        purgeConfig.transactionLowWatermark(transactionLowWatermark);
        return purgeConfig.build();
    }

}
