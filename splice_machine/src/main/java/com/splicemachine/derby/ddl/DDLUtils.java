package com.splicemachine.derby.ddl;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.catalog.DataDictionaryCache;
import com.splicemachine.db.impl.sql.catalog.TableKey;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.DerbyMessage;
import com.splicemachine.derby.impl.sql.execute.actions.ActiveTransactionReader;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.txn.LazyTxnView;
import com.splicemachine.storage.DataScan;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.StreamException;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.carrotsearch.hppc.BitSet;

/**
 * Created by jleach on 11/12/15.
 */
public class DDLUtils {
    private static final Logger LOG = Logger.getLogger(DDLUtils.class);

    public static DDLMessage.DDLChange performMetadataChange(DDLMessage.DDLChange ddlChange) throws StandardException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.trace(LOG,"performMetadataChange ddlChange=%s",ddlChange);
        notifyMetadataChangeAndWait(ddlChange);
        return ddlChange;
    }

    public static String notifyMetadataChange(DDLMessage.DDLChange ddlChange) throws StandardException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.trace(LOG,"notifyMetadataChange ddlChange=%s",ddlChange);
        return DDLDriver.driver().ddlController().notifyMetadataChange(ddlChange);
    }

    public static void finishMetadataChange(String changeId) throws StandardException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.trace(LOG,"finishMetadataChange changeId=%s",changeId);
        DDLDriver.driver().ddlController().finishMetadataChange(changeId);
    }


    public static void notifyMetadataChangeAndWait(DDLMessage.DDLChange ddlChange) throws StandardException{
        if (LOG.isDebugEnabled())
            SpliceLogUtils.trace(LOG,"notifyMetadataChangeAndWait ddlChange=%s",ddlChange);
        String changeId = notifyMetadataChange(ddlChange);
        if (LOG.isDebugEnabled())
            SpliceLogUtils.trace(LOG,"notifyMetadataChangeAndWait changeId=%s",changeId);
        DDLDriver.driver().ddlController().finishMetadataChange(changeId);
    }

    public static TxnView getLazyTransaction(long txnId) {
        //TODO -sf- could we remove this method somehow?
        SIDriver driver=SIDriver.driver();

        return new LazyTxnView(txnId, driver.getTxnSupplier(),driver.getExceptionFactory());
    }

    public static String outIntArray(int[] values) {
        return values==null?"null":Arrays.toString(values);
    }

    public static String outBoolArray(boolean[] values) {
        return values==null?"null":Arrays.toString(values);
    }



    public static Txn getIndexTransaction(TransactionController tc,
                                          Txn tentativeTransaction,
                                          long tableConglomId,
                                          String indexName) throws StandardException {
        final TxnView wrapperTxn = ((SpliceTransactionManager)tc).getActiveStateTxn();

        /*
         * We have an additional waiting transaction that we use to ensure that all elements
         * which commit after the demarcation point are committed BEFORE the populate part.
         */
        byte[] tableBytes = Long.toString(tableConglomId).getBytes();
        TxnLifecycleManager tlm = SIDriver.driver().lifecycleManager();
        Txn waitTxn;
        try{
            waitTxn = tlm.chainTransaction(wrapperTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION,false,tableBytes,tentativeTransaction);
        }catch(IOException ioe){
            LOG.error("Could not create a wait transaction",ioe);
            throw Exceptions.parseException(ioe);
        }

        //get the absolute user transaction
        TxnView uTxn = wrapperTxn;
        TxnView n = uTxn.getParentTxnView();
        while(n.getTxnId()>=0){
            uTxn = n;
            n = n.getParentTxnView();
        }
        // Wait for past transactions to die
        long oldestActiveTxn;
        try {
            oldestActiveTxn = waitForConcurrentTransactions(waitTxn, uTxn,tableConglomId);
        } catch (IOException e) {
            LOG.error("Unexpected error while waiting for past transactions to complete", e);
            throw Exceptions.parseException(e);
        }
        if (oldestActiveTxn>=0) {
            throw ErrorState.DDL_ACTIVE_TRANSACTIONS.newException("CreateIndex("+indexName+")",oldestActiveTxn);
        }
        Txn indexTxn;
        try{
            /*
             * We need to make the indexTxn a child of the wrapper, so that we can be sure
             * that the write pipeline is able to see the conglomerate descriptor. However,
             * this makes the SI logic more complex during the populate phase.
             */
            indexTxn = tlm.chainTransaction(wrapperTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION, true, tableBytes,waitTxn);
        } catch (IOException e) {
            LOG.error("Couldn't commit transaction for tentative DDL operation");
            // TODO must cleanup tentative DDL change
            throw Exceptions.parseException(e);
        }
        return indexTxn;
    }


    /**
     * Waits for concurrent transactions that started before the tentative
     * change completed.
     *
     * Performs an exponential backoff until a configurable timeout triggers,
     * then returns the list of transactions still running. The caller has to
     * forbid those transactions to ever write to the tables subject to the DDL
     * change.
     *
     * @param maximum
     *            wait for all transactions started before this one. It should
     *            be the transaction created just after the tentative change
     *            committed.
     * @param userTxn the <em>user-level</em> transaction of the ddl operation. It is important
     *                that it be the user-level, otherwise some child transactions may be treated
     *                as active when they are not actually active.
     * @return list of transactions still running after timeout
     * @throws IOException
     */
    public static long waitForConcurrentTransactions(Txn maximum, TxnView userTxn,long tableConglomId) throws IOException {
        byte[] conglomBytes = Long.toString(tableConglomId).getBytes();

        ActiveTransactionReader transactionReader = new ActiveTransactionReader(0l,maximum.getTxnId(),conglomBytes);
        SConfiguration config = SIDriver.driver().getConfiguration();
        Clock clock = SIDriver.driver().getClock();
        long waitTime = config.getLong(DDLConfiguration.DDL_REFRESH_INTERVAL); //the initial time to wait
        long maxWait = config.getLong(DDLConfiguration.MAX_DDL_WAIT); // the maximum time to wait
        long scale = 2; //the scale factor for the exponential backoff
        long timeAvailable = maxWait;
        long activeTxnId = -1l;
        do{
            try(Stream<TxnView> activeTxns = transactionReader.getActiveTransactions()){
                TxnView txn;
                while((txn = activeTxns.next())!=null){
                    if(!txn.descendsFrom(userTxn)){
                        activeTxnId = txn.getTxnId();
                    }
                }
            } catch (StreamException e) {
                throw new IOException(e.getCause());
            }
            if(activeTxnId<0) return activeTxnId;
            /*
             * It is possible for a sleep to pick up before the
             * waitTime is expired. Therefore, we measure that actual
             * time spent and use that for our time remaining period
             * instead.
             */
            long start = clock.currentTimeMillis();
            try {
                clock.sleep(waitTime,TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            long stop = clock.currentTimeMillis();
            timeAvailable-=(stop-start);
            /*
             * We want to exponentially back off, but only to the limit imposed on us. Once
             * our backoff exceeds that limit, we want to just defer to that limit directly.
             */
            waitTime = Math.min(timeAvailable,scale*waitTime);
        } while(timeAvailable>0);

        if (activeTxnId>=0) {
            LOG.warn(String.format("Running DDL statement %s. There are transaction still active: %d", "operation Running", activeTxnId));
        }
        return activeTxnId;
    }

    /**
     * Make sure that the table exists and that it isn't a system table. Otherwise, KA-BOOM
     */
    public static  void validateTableDescriptor(TableDescriptor td,String indexName, String tableName) throws StandardException {
        if (td == null)
            throw StandardException.newException(SQLState.LANG_CREATE_INDEX_NO_TABLE, indexName, tableName);
        if (td.getTableType() == TableDescriptor.SYSTEM_TABLE_TYPE)
            throw StandardException.newException(SQLState.LANG_CREATE_SYSTEM_INDEX_ATTEMPTED, indexName, tableName);
    }

    /**
     *
     * Create a table scan for old conglomerate. Make sure to create a NonSI table scan. Transaction filtering
     * will happen at client side
     * @return
     */
    public static DataScan createFullScan() {
        DataScan scan = SIDriver.driver().getOperationFactory().newDataScan(null);
        scan.startKey(SIConstants.EMPTY_BYTE_ARRAY).stopKey(SIConstants.EMPTY_BYTE_ARRAY).returnAllVersions();
        return scan;
    }

    public static int[] getMainColToIndexPosMap(int[] indexColsToMainColMap, BitSet indexedCols) {
        int[] mainColToIndexPosMap = new int[(int) indexedCols.length()];
        for (int i = 0 ; i < indexColsToMainColMap.length; ++i) {
            mainColToIndexPosMap[i] = -1;
        }
        for (int indexCol = 0; indexCol < indexColsToMainColMap.length; indexCol++) {
            int mainCol = indexColsToMainColMap[indexCol];
            mainColToIndexPosMap[mainCol - 1] = indexCol;
        }
        return mainColToIndexPosMap;
    }

    public static BitSet getIndexedCols(int[] indexColsToMainColMap) {
        BitSet indexedCols = new BitSet();
        for (int indexCol : indexColsToMainColMap) {
            indexedCols.set(indexCol - 1);
        }
        return indexedCols;
    }

    public static byte[] getIndexConglomBytes(long indexConglomerate) {
        return Bytes.toBytes(Long.toString(indexConglomerate));
    }

    public static byte[] serializeColumnInfoArray(ColumnInfo[] columnInfos) throws StandardException {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeInt(columnInfos.length);
            for (int i =0; i< columnInfos.length;i++) {
                oos.writeObject(columnInfos[i]);
            }
            oos.flush();
            oos.close();
            return baos.toByteArray();
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    public static ColumnInfo[] deserializeColumnInfoArray(byte[] bytes) {
        ObjectInputStream oos = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream is = new ObjectInputStream(bis);
            ColumnInfo[] columnInfos = new ColumnInfo[is.readInt()];
            for (int i =0; i< columnInfos.length;i++) {
                columnInfos[i] = (ColumnInfo) is.readObject();
            }
            is.close();
            return columnInfos;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     *
     * Prepare all dependents to invalidate.  (There is a chance
     * to say that they can't be invalidated.  For example, an open
     * cursor referencing a table/view that the user is attempting to
     * drop.) If no one objects, then invalidate any dependent objects.
     * We check for invalidation before we drop the table descriptor
     * since the table descriptor may be looked up as part of
     * decoding tuples in SYSDEPENDS.
     *
     *
     * @param change
     * @param dd
     * @param dm
     * @throws StandardException
     */
    public static void preDropTable(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException{
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preDropTable with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
//            ContextManager currentCm = ContextService.getFactory().getCurrentContextManager();
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean prepared = transactionResource.marshallTransaction(txn);
            TableDescriptor td = dd.getTableDescriptor(ProtoUtil.getDerbyUUID(change.getDropTable().getTableId()));
            if (td==null) // Table Descriptor transaction never committed
                return;
            flushCachesBasedOnTableDescriptor(td,dd);
            dm.invalidateFor(td, DependencyManager.DROP_TABLE, transactionResource.getLcc());
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    public static void preDropSchema(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException{
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preDropTable with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean initializedTxn = false;
            try{
                initializedTxn = transactionResource.marshallTransaction(txn);

                SchemaDescriptor sd=dd.getSchemaDescriptor(change.getDropSchema().getSchemaName(),transactionResource.getLcc().getTransactionExecute(),false);
                if(sd==null) // Table Descriptor transaction never committed
                    return;
                flushCachesBasedOnSchemaDescriptor(sd,dd);
                dm.invalidateFor(sd,DependencyManager.DROP_SCHEMA,transactionResource.getLcc());
            }finally{
                if(initializedTxn)
                    transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    public static void preAlterStats(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException{
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preDropTable with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
//            ContextManager currentCm = ContextService.getFactory().getCurrentContextManager();
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean prepared = transactionResource.marshallTransaction(txn);
            List<DerbyMessage.UUID> tdUIDs = change.getAlterStats().getTableIdList();
            for (DerbyMessage.UUID uuuid : tdUIDs) {
                TableDescriptor td = dd.getTableDescriptor(ProtoUtil.getDerbyUUID(uuuid));
                if (td==null) // Table Descriptor transaction never committed
                    return;
                flushCachesBasedOnTableDescriptor(td,dd);
                dm.invalidateFor(td, DependencyManager.DROP_TABLE, transactionResource.getLcc());
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    public static void preCreateIndex(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException {
        preIndex(change, dd, dm, DependencyManager.CREATE_INDEX, change.getTentativeIndex().getTable().getTableUuid());
    }

    public static void preDropIndex(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException {
        preIndex(change, dd, dm, DependencyManager.DROP_INDEX, change.getDropIndex().getTableUUID());
    }

    private static void preIndex(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm, int action, DerbyMessage.UUID uuid) throws StandardException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preIndex with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean initializedTxn = false;
            try {
                initializedTxn = transactionResource.marshallTransaction(txn);
                TableDescriptor td = dd.getTableDescriptor(ProtoUtil.getDerbyUUID(uuid));
                if (td == null) // Table Descriptor transaction never committed
                    return;
                flushCachesBasedOnTableDescriptor(td,dd);
                dm.invalidateFor(td, action, transactionResource.getLcc());
            } finally {
                if (initializedTxn)
                    transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    private static void flushCachesBasedOnTableDescriptor(TableDescriptor td,DataDictionary dd) throws StandardException {
        DataDictionaryCache cache = dd.getDataDictionaryCache();
        TableKey tableKey = new TableKey(td.getSchemaDescriptor().getUUID(),td.getName());
        cache.nameTdCacheRemove(tableKey);
        cache.oidTdCacheRemove(td.getUUID());
        // Remove Conglomerate Level and Statistics Caching..
        for (ConglomerateDescriptor cd: td.getConglomerateDescriptorList()) {
            cache.partitionStatisticsCacheRemove(cd.getConglomerateNumber());
            cache.conglomerateCacheRemove(cd.getConglomerateNumber());
        }
    }

    private static void flushCachesBasedOnSchemaDescriptor(SchemaDescriptor sd,DataDictionary dd) throws StandardException {
        DataDictionaryCache cache = dd.getDataDictionaryCache();
        cache.schemaCacheRemove(sd.getSchemaName());
//        TableKey tableKey = new TableKey(td.getSchemaDescriptor().getUUID(),td.getName());
//        cache.nameTdCacheRemove(tableKey);
//        cache.oidTdCacheRemove(td.getUUID());
//        // Remove Conglomerate Level and Statistics Caching..
//        for (ConglomerateDescriptor cd: td.getConglomerateDescriptorList()) {
//            cache.partitionStatisticsCacheRemove(cd.getConglomerateNumber());
//            cache.conglomerateCacheRemove(cd.getConglomerateNumber());
//        }
    }

}
