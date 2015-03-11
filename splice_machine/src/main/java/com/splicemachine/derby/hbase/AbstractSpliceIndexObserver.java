package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.async.Bytes;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.temp.TempTable;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.backup.Backup;
import com.splicemachine.hbase.backup.SnapshotUtils;
import com.splicemachine.hbase.backup.SnapshotUtilsFactory;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.MemstoreAware;
import com.splicemachine.hbase.backup.BackupUtils;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.writecontextfactory.WriteContextFactory;
import com.splicemachine.pipeline.constraint.Constraint;
import com.splicemachine.pipeline.constraint.ConstraintViolation;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.pipeline.writecontextfactory.WriteContextFactoryManager;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnOperationFactory;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.SimpleOperationFactory;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.si.impl.WriteConflict;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.sql.ResultSet;

/**
 * Region Observer for managing indices.
 *
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public abstract class AbstractSpliceIndexObserver extends BaseRegionObserver {
    private static final Logger LOG = Logger.getLogger(AbstractSpliceIndexObserver.class);
    protected AtomicReference<MemstoreAware> memstoreAware = new AtomicReference<MemstoreAware>(new MemstoreAware());     // Atomic Reference to memstore aware state handling
    protected DerbyFactory factory = DerbyFactoryDriver.derbyFactory;
    
    /**
     * Log component specific to compaction related code, so that this functionality
     * can be logged in isolation without having to also see the rest of the logging
     * from this class. To see additional temp table compaction logging,
     * also use log component: com.splicemachine.derby.impl.temp.TempTable.
     */
    private static final Logger LOG_COMPACT = Logger.getLogger(AbstractSpliceIndexObserver.class.getName() + ".Compaction");

    private static final String LOG_COMPACT_PRE = TempTable.LOG_COMPACT_PRE;

    protected long conglomId;
    protected boolean isTemp;
    protected long blockingStoreFiles;
    protected TransactionalRegion region;
    protected TxnOperationFactory operationFactory;

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        super.stop(e);
        if (region != null)
            region.discard();
    }

    @Override
    public void postOpen(final ObserverContext<RegionCoprocessorEnvironment> e) {
        //get the Conglomerate Id. If it's not a table that we can index (e.g. META, ROOT, SYS_TEMP,__TXN_LOG, etc)
        //then don't bother with setting things up
        String tableName = e.getEnvironment().getRegion().getTableDesc().getNameAsString();
        try {
            conglomId = Long.parseLong(tableName);

            operationFactory = new SimpleOperationFactory();
            region = TransactionalRegions.get(e.getEnvironment().getRegion());
        } catch (NumberFormatException nfe) {
            SpliceLogUtils.debug(LOG, "Unable to parse Conglomerate Id for table %s, indexing is will not be set up", tableName);
            conglomId = -1;
            blockingStoreFiles = SpliceConstants.config.getInt("hbase.hstore.blockingStoreFiles", 10);
            isTemp = SpliceConstants.TEMP_TABLE.equals(tableName);
            return;
        }

        super.postOpen(e);
    }

    @Override
    public InternalScanner preCompactScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
                                                 Store store,
                                                 List<? extends KeyValueScanner> scanners,
                                                 ScanType scanType,
                                                 long earliestPutTs, InternalScanner s) throws IOException {
        if (!isTemp || (s == null && scanners.size() <= 0))
            return super.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s);

        if (blockingStoreFiles <= scanners.size()) {
            LOG_COMPACT.info(String.format("%s Falling back to normal HBase compaction for TEMP. Consider increasing setting for blockingStoreFiles.", LOG_COMPACT_PRE));
            return super.preCompactScannerOpen(c, store, scanners, scanType, earliestPutTs, s);
        } else {
            if (LOG_COMPACT.isTraceEnabled())
                LOG_COMPACT.trace(String.format("%s Compacting TEMP using the TEMP compaction mechanism", LOG_COMPACT_PRE));
            c.complete();
            return SpliceDriver.driver().getTempTable().getTempCompactionScanner();
        }
    }

    @Override
    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<StoreFile> candidates) throws IOException {
        filterFiles(c, candidates);
    }

    @Override
    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store, List<StoreFile> candidates, CompactionRequest request) throws IOException {
        filterFiles(c, candidates);
    }

    @Override
    public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,
                            Store store, StoreFile resultFile, CompactionRequest request) throws IOException {

        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "postCompact store=%s, storeFile=%s, request=%s", store, resultFile, request);

        recordCompactedFilesForBackup(e, request, resultFile);

        while (true) {
            MemstoreAware latest = memstoreAware.get();
            if (memstoreAware.compareAndSet(latest, MemstoreAware.decrementCompactionCount(latest))) ;
            break;
        }
        super.postCompact(e, store, resultFile, request);
    }
    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r) throws IOException {
        recordRegionSplitForBackup(e, l, r);
    }
    /**
     * ***************************************************************************************************************
     */
    /*private helper methods*/

    private void updateFileSet(String tableName, String parentRegionName, Set<String> pathSet,
                               HashMap<String, Collection<StoreFileInfo>> storeFileInfoMap, HRegion r) {
        if (LOG.isTraceEnabled()) {
            SpliceLogUtils.info(LOG, "updateFileSet: region = " + r.getRegionInfo().getEncodedName());
        }
        String childRegionName = r.getRegionInfo().getEncodedName();
        for(Collection<StoreFileInfo> storeFiles : storeFileInfoMap.values()) {
            for (StoreFileInfo storeFile : storeFiles) {
                String referenceFileName = storeFile.getPath().getName();
                String[] s = referenceFileName.split("\\.");
                if (LOG.isTraceEnabled()) {
                    SpliceLogUtils.info(LOG, "updateFileSet: referenceFileName = " + referenceFileName);
                    SpliceLogUtils.info(LOG, "updateFileSet: referenced file = " + s[0]);
                }
                if (pathSet.contains(s[0])) {
                    // If referenced file appears in the last snapshot, then reference file in child region
                    // should not be included in next incremental backup
                    BackupUtils.insertFileSet(tableName, childRegionName, referenceFileName, false);
                }
                BackupUtils.FileSet fileSet = BackupUtils.getFileSet(tableName, parentRegionName, s[0]);
                if (fileSet != null) {
                    // If there is an entry in Backup.FileSet for the referenceD file in parent region, an entry should
                    // be created for reference file in child region
                    BackupUtils.insertFileSet(fileSet.getTableName(), childRegionName,
                            referenceFileName, fileSet.shouldInclude());
                }
            }
        }
    }

    private void recordRegionSplitForBackup(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r) {
        try {

            // Nothing needs to be done if there was never a succeeded backup
            boolean exists = BackupUtils.existBackupWithStatus(Backup.BackupStatus.S.toString());
            if (!exists)
                return;
            if (LOG.isTraceEnabled()) {
                SpliceLogUtils.info(LOG, "postSplit: parent region " + e.getEnvironment().getRegion().getRegionInfo().getEncodedName());
                SpliceLogUtils.info(LOG, "postSplit: l region " + l.getRegionInfo().getEncodedName());
                SpliceLogUtils.info(LOG, "postSplit: r region " + r.getRegionInfo().getEncodedName());
            }

            String tableName = e.getEnvironment().getRegion().getTableDesc().getNameAsString();
            String encodedRegionName = e.getEnvironment().getRegion().getRegionInfo().getEncodedName();

            String snapshotName = BackupUtils.getLastSnapshotName(tableName);
            Configuration conf = SpliceConstants.config;
            HRegion region = e.getEnvironment().getRegion();
            FileSystem fs = region.getFilesystem();
            Path rootDir = FSUtils.getRootDir(conf);
            SnapshotUtils utils = SnapshotUtilsFactory.snapshotUtils;
            Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
            List<Path> pathList = utils.getSnapshotFilesForRegion(region, conf, fs, snapshotDir);
            Set<String> pathSet = new HashSet<>();
            for (Path p : pathList) {
                String name = p.getName();
                pathSet.add(name);
            }

            HashMap<String, Collection<StoreFileInfo>> lStoreFileInfoMap = BackupUtils.getStoreFileInfo(l);
            HashMap<String, Collection<StoreFileInfo>> rStoreFileInfoMap = BackupUtils.getStoreFileInfo(r);

            updateFileSet(tableName, encodedRegionName, pathSet, lStoreFileInfoMap, l);
            updateFileSet(tableName, encodedRegionName, pathSet, rStoreFileInfoMap, r);
            //BackupUtils.deleteFileSetForRegion(tableName, encodedRegionName);
            BackupUtils.recordRegionSplit(e, l, r);

        }catch (Exception ex) {
            SpliceLogUtils.warn(LOG, "Error in recordRegionSplitForBackup");
        }
    }

    private void recordCompactedFilesForBackup(ObserverContext<RegionCoprocessorEnvironment> e,
                                               CompactionRequest request, StoreFile resultFile) {

        // During compaction, an HFile may need to be registered to Backup.FileSet for incremental backup
        // Check HFiles in compaction request, and classify them into 2 categories
        // 1. HFile that contains new data only. a HFile does not appear in the last snapshot, and was not a result of
        //    a recent(after last snapshot) compaction. For latter case, Backup.FileSet contains a row for the HFile
        //    and indicate it should be excluded.
        // 2. HFile that contains old data. A HFile either appears in the last snapshot, or was a result of a recent
        //    compaction.
        //
        // If the set of HFiles are all in category 1, there is no need to keep track of HFiles. The resultant HFile
        // contains new data only, and the next incremental backup will copy it.
        // If the set of HFiles are all in category 2, insert an entry into BACKUP.FileSet for the resultant HFile, and
        // indicate it should be excluded for next incremental backup.
        // If the set of HFile are in both category 1 and 2. Then for each HFile in category 1, insert an entry and
        // indicate it should be included for next incremental backup. Insert an entry for resultant HFile to indicate
        // it should be excluded for next incremental backup.
        try {
            // Nothing needs to be done if there was never a succeeded backup
            boolean exists = BackupUtils.existBackupWithStatus(Backup.BackupStatus.S.toString());
            if (!exists)
                return;

            String tableName = e.getEnvironment().getRegion().getTableDesc().getNameAsString();
            String snapshotName = BackupUtils.getLastSnapshotName(tableName);
            Set<String> pathSet = new HashSet<>();
            String encodedRegionName = e.getEnvironment().getRegion().getRegionInfo().getEncodedName();
            SnapshotUtils utils = SnapshotUtilsFactory.snapshotUtils;
            HRegion region = e.getEnvironment().getRegion();
            FileSystem fs = region.getFilesystem();
            if (snapshotName != null) {
                Configuration conf = SpliceConstants.config;
                Path rootDir = FSUtils.getRootDir(conf);
                Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
                List<Path> pathList = utils.getSnapshotFilesForRegion(region, conf, fs, snapshotDir);

                for (Path path : pathList) {
                    pathSet.add(path.getName());
                }
            }
            List<StoreFile> oldHFile = new ArrayList<>();
            List<StoreFile> newHFile = new ArrayList<>();
            Collection<StoreFile> storeFiles = request.getFiles();
            for(StoreFile storeFile : storeFiles) {
                Path p = storeFile.getPath();
                String name = p.getName();
                if(pathSet.contains(name)) {
                    oldHFile.add(storeFile);
                }
                else if (BackupUtils.shouldExclude(tableName, encodedRegionName, p.getName()) ||
                         BackupUtils.shouldExcludeReferencedFile(tableName, p.getName(), fs)) {
                    oldHFile.add(storeFile);
                }
                else {
                    newHFile.add(storeFile);
                }
            }

            if (newHFile.size() == 0 && oldHFile.size() > 0) {
                BackupUtils.insertFileSet(tableName, encodedRegionName, resultFile.getPath().getName(), false);
                for(StoreFile storeFile : oldHFile) {
                    // These HFile will be removed to archived directory and purged.
                    BackupUtils.deleteFileSet(tableName, encodedRegionName, storeFile.getPath().getName(), false);
                }
            }
            else if (newHFile.size() > 0 && oldHFile.size() > 0) {
                for (StoreFile storeFile : newHFile) {
                    BackupUtils.insertFileSet(tableName, encodedRegionName, storeFile.getPath().getName(), true);
                }
                BackupUtils.insertFileSet(tableName, encodedRegionName, resultFile.getPath().getName(), false);
                for (StoreFile storeFile:oldHFile) {
                    // These HFile will be removed to archived directory and purged.
                    BackupUtils.deleteFileSet(tableName, encodedRegionName, storeFile.getPath().getName(), false);
                }
            }
        }
        catch (Exception ex) {
            SpliceLogUtils.warn(LOG, "postCompact: cannot query last backup");
        }
    }

    private void filterFiles(ObserverContext<RegionCoprocessorEnvironment> c, List<StoreFile> candidates) throws IOException {
        /*
         * We want to remove TEMP files that we aren't interested in anymore. However, there is always the possibility
         * that these files can't be removed--that we're always interested in them. Normally, this is fine, we just
         * don't perform any compaction.
         *
         * However, once we exceed the blockingStoreFiles limit of files, then we are in trouble--we need to fall
         * back on SOME kind of compaction. Therefore, if we have at least as many candidate files as blockingStoreFiles,
         * AND we are unable to remove any from TEMP exposure, then retain them ALL and fall back to normal compaction.
         */
        int numCandidates = candidates.size();
        if (!isTemp || numCandidates <= 0) {
            return;
        }
        if (LOG_COMPACT.isTraceEnabled())
            LOG_COMPACT.trace(String.format("%s Checking for removable files in list of %d", LOG_COMPACT_PRE, numCandidates));
        List<StoreFile> copy = Lists.newArrayList(candidates);
        try {
            SpliceDriver.driver().getTempTable().filterCompactionFiles(c.getEnvironment().getConfiguration(), copy);
            if (copy.size() == 0) { // All candidates have been removed from list - can't delete any files
                /*
                 * We need to keep all the files around. This leaves two situations: when we have exceeded the
                 * blocking store files and when we have not.
                 *
                 * When we have not exceeded blockingStoreFiles, then we can just behave like normal--leave
                 * the files around. This means that we clear out the candidates list and return.
                 *
                 * When we exceed blockingStoreFiles, we keep candidates the same, and we will detect that the
                 * number has exceeded in the preCompactScannerOpen and fall back to the default.
                 */
                if (numCandidates < blockingStoreFiles) {
                    //we are free to use the normal TEMP compaction procedure, which does nothing.
                    // TODO: Consider compacting some files even if we have not hit blocking limit yet
                    candidates.clear();
                } else {
                    // if the above isn't met, then we do nothing to candidates, because we are falling back to normal TEMP
                    if (LOG_COMPACT.isDebugEnabled())
                        LOG_COMPACT.debug(String.format(
                                "%s No removable files found in list of %d. BlockingStoreFiles limit %d exceeded, so proceeding with default HBase compaction",
                                LOG_COMPACT_PRE, numCandidates, blockingStoreFiles));
                }
            } else {
                // there are at least some files to remove using the TEMP structure, so just go with it
                if (LOG_COMPACT.isDebugEnabled())
                    LOG_COMPACT.debug(String.format(
                            "%s %d removable files found in candidate list of %d.",
                            LOG_COMPACT_PRE, copy.size(), numCandidates));
                candidates.retainAll(copy);
            }

            c.bypass();
            c.complete();
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }
    }

    protected void mutate(RegionCoprocessorEnvironment rce, KVPair mutation, TxnView txn) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "mutate %s", mutation);
        //we've already done our write path, so just pass it through
        WriteContextFactory<TransactionalRegion> ctxFactory = WriteContextFactoryManager.getWriteContext(conglomId);

        try {
            WriteContext context = ctxFactory.createPassThrough(null, txn, region, 1, null);
            context.sendUpstream(mutation);
            context.flush();
            WriteResult mutationResult = context.close().get(mutation);
            if (mutationResult == null) {
                return; //we didn't actually do anything, so no worries
            }
            switch (mutationResult.getCode()) {
                case FAILED:
                    throw new IOException(mutationResult.getErrorMessage());
                case PRIMARY_KEY_VIOLATION:
                    throw ConstraintViolation.create(Constraint.Type.PRIMARY_KEY, mutationResult.getConstraintContext());
                case UNIQUE_VIOLATION:
                    throw ConstraintViolation.create(Constraint.Type.UNIQUE, mutationResult.getConstraintContext());
                case FOREIGN_KEY_VIOLATION:
                    throw ConstraintViolation.create(Constraint.Type.FOREIGN_KEY, mutationResult.getConstraintContext());
                case CHECK_VIOLATION:
                    throw ConstraintViolation.create(Constraint.Type.CHECK, mutationResult.getConstraintContext());
                case WRITE_CONFLICT:
                    throw WriteConflict.fromString(mutationResult.getErrorMessage());
                case NOT_RUN:
                case SUCCESS:
                default:
                    break;
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            ctxFactory.close();
        }
    }
    
    private boolean startRowInRange(ObserverContext<RegionCoprocessorEnvironment> c, byte[] startRow) {
    	return HRegion.rowIsInRange(c.getEnvironment().getRegion().getRegionInfo(), startRow);
    }

    private boolean stopRowInRange(ObserverContext<RegionCoprocessorEnvironment> c, byte[] stopRow) {
    	return HRegion.rowIsInRange(c.getEnvironment().getRegion().getRegionInfo(), stopRow)
    			|| Bytes.equals(c.getEnvironment().getRegion().getRegionInfo().getEndKey(),stopRow);
    	
    }

    @Override
	public KeyValueScanner preStoreScannerOpen(
			ObserverContext<RegionCoprocessorEnvironment> c, Store store,
			Scan scan, NavigableSet<byte[]> targetCols, KeyValueScanner s)
			throws IOException {
		if (scan.getAttribute(MRConstants.SPLICE_SCAN_MEMSTORE_ONLY) != null &&
				Bytes.equals(scan.getAttribute(MRConstants.SPLICE_SCAN_MEMSTORE_ONLY), SIConstants.TRUE_BYTES)) {			
			if(LOG.isDebugEnabled()){
				SpliceLogUtils.debug(LOG, "preStoreScannerOpen in MR mode %s", 
						c.getEnvironment().getRegion() );
			}
			if(LOG.isDebugEnabled()){
				SpliceLogUtils.debug(LOG, "scan Check Code scan=%s, startKey {value=%s, inRange=%s}, endKey {value=%s, inRange=%s}",scan ,
						scan.getStartRow(), startRowInRange(c, scan.getStartRow()),
						scan.getStopRow(), stopRowInRange(c, scan.getStopRow()));
			}
			
			// Throw Retry Exception if the region is splitting
			
			
	        while (true) {
				MemstoreAware currentState = memstoreAware.get();
				if (currentState.splitMerge || currentState.compactionCount>0) {
					SpliceLogUtils.warn(LOG, "splitting, merging, or active compaction on scan on %s",c.getEnvironment().getRegion().getRegionNameAsString());
					throw new DoNotRetryIOException();
				}					
	            if (memstoreAware.compareAndSet(currentState, MemstoreAware.incrementScannerCount(currentState)));
	                break;
	        }
				if (!startRowInRange(c, scan.getStartRow()) ||
						!stopRowInRange(c, scan.getStopRow())) {
					while (true) {
						MemstoreAware latest = memstoreAware.get();
						if(memstoreAware.compareAndSet(latest, MemstoreAware.decrementScannerCount(latest)));
							break;
					}
					SpliceLogUtils.warn(LOG, "scan missed do to split after task creation beginKey=%s, endKey=%s, region=%s",scan.getStartRow(), scan.getStopRow(),c.getEnvironment().getRegion().getRegionNameAsString());
					throw new DoNotRetryIOException();
				}
			
			InternalScan iscan = new InternalScan(scan);
			iscan.checkOnlyMemStore();
			return factory.getMemstoreFlushAwareScanner(c.getEnvironment().getRegion(),store, store.getScanInfo(), iscan, targetCols,
					getReadpoint(c.getEnvironment().getRegion()),memstoreAware,memstoreAware.get());
		}		
		return super.preStoreScannerOpen(c, store, scan, targetCols, s);
	}	

    protected abstract long getReadpoint(HRegion region);
    
	public void preFlush() {
		while (true) {
			MemstoreAware latest = memstoreAware.get();
			if(memstoreAware.compareAndSet(latest, MemstoreAware.incrementFlushCount(latest)));
				break;
		}		
	}
	
	public void preSplit() throws IOException {
		while (true) {
			MemstoreAware latest = memstoreAware.get();
			if(memstoreAware.compareAndSet(latest, MemstoreAware.changeSplitMerge(latest, true)));
				break;
		}		
		while (memstoreAware.get().scannerCount>0) {
			SpliceLogUtils.warn(LOG, "preSplit Delayed waiting for scanners to complete scannersRemaining=%d",memstoreAware.get().scannerCount);
			try {
				Thread.sleep(1000); // Have Split sleep for a second
			} catch (InterruptedException e1) {
				throw new IOException(e1);
			}
		}
	}

	public void preCompact() throws IOException {
		while (true) {
			MemstoreAware latest = memstoreAware.get();
			if(memstoreAware.compareAndSet(latest, MemstoreAware.incrementCompactionCount(latest)));
				break;
		}
		while (memstoreAware.get().scannerCount>0) {
			SpliceLogUtils.warn(LOG, "compaction Delayed waiting for scanners to complete scannersRemaining=%d",memstoreAware.get().scannerCount);
			try {
				Thread.sleep(1000); // Have Split sleep for a second
			} catch (InterruptedException e1) {
				throw new IOException(e1);
			}
		}
	}

	public void postCompact() {
		while (true) {
			MemstoreAware latest = memstoreAware.get();
			if(memstoreAware.compareAndSet(latest, MemstoreAware.decrementCompactionCount(latest)));
				break;
		}
	}
	
	@Override
	public void preFlush(ObserverContext<RegionCoprocessorEnvironment> e)
			throws IOException {
		SpliceLogUtils.trace(LOG, "preFlush called");
		super.preFlush(e);
	}

	
	
	@Override
	public InternalScanner preFlush(
			ObserverContext<RegionCoprocessorEnvironment> e, Store store,
			InternalScanner scanner) throws IOException {
		SpliceLogUtils.trace(LOG, "preFlush called on store %s",store);
		preFlush();
		return super.preFlush(e, store, scanner);
	}
	
	

	@Override
	public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e)
			throws IOException {
		SpliceLogUtils.trace(LOG, "postFlush called");
		super.postFlush(e);
	}

	@Override
	public void postFlush(ObserverContext<RegionCoprocessorEnvironment> e,
			Store store, StoreFile resultFile) throws IOException {
		SpliceLogUtils.trace(LOG, "postFlush called on store %s with file=%s",store, resultFile);
		super.postFlush(e, store, resultFile);
	}
	
    @Override
	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e)
			throws IOException {
		SpliceLogUtils.trace(LOG, "preSplit");	
		preSplit();
    	super.preSplit(e);
	}

	@Override
	public void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,
			Store store, StoreFile resultFile) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "postCompact store=%s, storeFile=%s", store, resultFile);
        while (true) {
            MemstoreAware latest = memstoreAware.get();
            if (memstoreAware.compareAndSet(latest, MemstoreAware.decrementCompactionCount(latest))) ;
            break;
        }
        super.postCompact(e, store, resultFile);
    }
}
