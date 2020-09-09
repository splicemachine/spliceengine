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

package com.splicemachine.derby.ddl;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.catalog.DataDictionaryCache;
import com.splicemachine.db.impl.sql.compile.ColumnDefinitionNode;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.DerbyMessage;
import com.splicemachine.derby.impl.sql.execute.actions.ActiveTransactionReader;
import com.splicemachine.derby.impl.sql.execute.actions.DropAliasConstantOperation;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.txn.LazyTxnView;
import com.splicemachine.storage.DataScan;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.StreamException;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by jleach on 11/12/15.
 *
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



    public static Txn getIndexTransaction(TransactionController tc, Txn tentativeTransaction, long tableConglomId, String indexName) throws StandardException {
        final TxnView wrapperTxn = ((SpliceTransactionManager)tc).getActiveStateTxn();

        /*
         * We have an additional waiting transaction that we use to ensure that all elements
         * which commit after the demarcation point are committed BEFORE the populate part.
         */
        byte[] tableBytes = Bytes.toBytes(Long.toString(tableConglomId));
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
        byte[] conglomBytes = Bytes.toBytes(Long.toString(tableConglomId));

        ActiveTransactionReader transactionReader = new ActiveTransactionReader(0l,maximum.getTxnId(),conglomBytes);
        SConfiguration config = SIDriver.driver().getConfiguration();
        Clock clock = SIDriver.driver().getClock();
        long waitTime = config.getDdlRefreshInterval(); //the initial time to wait
        long maxWait = config.getMaxDdlWait(); // the maximum time to wait
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
        for (int i = 0 ; i < indexedCols.length(); ++i) {
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
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean prepared = false;
            try{
                prepared=transactionResource.marshallTransaction(txn);
                TableDescriptor td=dd.getTableDescriptor(ProtoUtil.getDerbyUUID(change.getDropTable().getTableId()));
                if(td==null) // Table Descriptor transaction never committed
                    return;
                dm.invalidateFor(td,DependencyManager.DROP_TABLE,transactionResource.getLcc());
            }finally{
               if(prepared)
                   transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    public static void preAlterStats(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException{
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preAlterStats with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean prepared = false;
            try{
                prepared=transactionResource.marshallTransaction(txn);
                List<DerbyMessage.UUID> tdUIDs=change.getAlterStats().getTableIdList();
                for(DerbyMessage.UUID uuuid : tdUIDs){
                    TableDescriptor td=dd.getTableDescriptor(ProtoUtil.getDerbyUUID(uuuid));
                    if(td==null) // Table Descriptor transaction never committed
                        return;
                    dm.invalidateFor(td,DependencyManager.DROP_STATISTICS,transactionResource.getLcc());
                }
            }finally{
                if(prepared)
                    transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    public static void preDropSchema(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preDropSchema with change=%s",change);
        dd.getDataDictionaryCache().schemaCacheRemove(change.getDropSchema().getSchemaName());
        dd.getDataDictionaryCache().oidSchemaCacheRemove(ProtoUtil.getDerbyUUID(change.getDropSchema().getSchemaUUID()));

    }

    public static void preUpdateSchemaOwner(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preUpdateSchemaOwner with change=%s",change);

        SpliceTransactionResourceImpl transactionResource = null;
        boolean prepared = false;
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            transactionResource = new SpliceTransactionResourceImpl();
            prepared = transactionResource.marshallTransaction(txn);
            // remove corresponding schema canche entry
            dd.getDataDictionaryCache().schemaCacheRemove(change.getUpdateSchemaOwner().getSchemaName());
            dd.getDataDictionaryCache().oidSchemaCacheRemove(ProtoUtil.getDerbyUUID(change.getUpdateSchemaOwner().getSchemaUUID()));
            // clear permission cache as it has out-of-date permission info for the schema
            dd.getDataDictionaryCache().clearPermissionCache();
            // clear  TableDescriptor cache as it may reference the schema with an out-of-date authorization id
            dd.getDataDictionaryCache().clearOidTdCache();
            dd.getDataDictionaryCache().clearNameTdCache();
        } catch (Exception e) {
            e.printStackTrace();
            throw StandardException.plainWrapException(e);
        } finally {
            if (prepared) {
                transactionResource.close();
            }
        }
    }

    public static void preCreateIndex(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException {
        preIndex(change, dd, dm, DependencyManager.CREATE_INDEX, change.getTentativeIndex().getTable().getTableUuid());
    }

    public static void preDropIndex(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preDropIndex with change=%s",change);
        boolean prepared = false;
        SpliceTransactionResourceImpl transactionResource = null;
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            transactionResource = new SpliceTransactionResourceImpl();
            //transactionResource.prepareContextManager();
            prepared = transactionResource.marshallTransaction(txn);
            TransactionController tc = transactionResource.getLcc().getTransactionExecute();
            DDLMessage.DropIndex dropIndex =  change.getDropIndex();
            SchemaDescriptor sd = dd.getSchemaDescriptor(dropIndex.getSchemaName(),tc,true);
            TableDescriptor td = dd.getTableDescriptor(ProtoUtil.getDerbyUUID(dropIndex.getTableUUID()));
            ConglomerateDescriptor cd = dd.getConglomerateDescriptor(dropIndex.getIndexName(), sd, true);
            if (td!=null) { // Table Descriptor transaction never committed
                dm.invalidateFor(td, DependencyManager.ALTER_TABLE, transactionResource.getLcc());
            }
            if (cd!=null) {
                dm.invalidateFor(cd, DependencyManager.DROP_INDEX, transactionResource.getLcc());
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        } finally {
            if (prepared)
                transactionResource.close();
        }
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
                if (td==null) // Table Descriptor transaction never committed
                    return;
                dm.invalidateFor(td, action, transactionResource.getLcc());
            } finally {
                if (initializedTxn)
                    transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    public static void preRenameTable(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preRenameTable with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean prepared = false;
            try{
                prepared = transactionResource.marshallTransaction(txn);
                DerbyMessage.UUID uuuid=change.getRenameTable().getTableId();
                TableDescriptor td=dd.getTableDescriptor(ProtoUtil.getDerbyUUID(uuuid));
                if(td==null) // Table Descriptor transaction never committed
                    return;
                dm.invalidateFor(td,DependencyManager.RENAME,transactionResource.getLcc());
    		/* look for foreign key dependency on the table. If found any,
	    	use dependency manager to pass the rename action to the
		    dependents. */
                ConstraintDescriptorList constraintDescriptorList=dd.getConstraintDescriptors(td);
                for(int index=0;index<constraintDescriptorList.size();index++){
                    ConstraintDescriptor constraintDescriptor=constraintDescriptorList.elementAt(index);
                    if(constraintDescriptor instanceof ReferencedKeyConstraintDescriptor)
                        dm.invalidateFor(constraintDescriptor,DependencyManager.RENAME,transactionResource.getLcc());
                }
            }finally{
                if(prepared)
                    transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    public static void preRenameColumn(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException  {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preRenameColumn with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean prepared = false;
            try{
                prepared=transactionResource.marshallTransaction(txn);
                DerbyMessage.UUID uuuid=change.getRenameColumn().getTableId();
                TableDescriptor td=dd.getTableDescriptor(ProtoUtil.getDerbyUUID(uuuid));
                if(td==null) // Table Descriptor transaction never committed
                    return;
                ColumnDescriptor columnDescriptor=td.getColumnDescriptor(change.getRenameColumn().getColumnName());
                if(columnDescriptor.isAutoincrement())
                    columnDescriptor.setAutoinc_create_or_modify_Start_Increment(
                            ColumnDefinitionNode.CREATE_AUTOINCREMENT);

                int columnPosition=columnDescriptor.getPosition();
                FormatableBitSet toRename=new FormatableBitSet(td.getColumnDescriptorList().size()+1);
                toRename.set(columnPosition);
                td.setReferencedColumnMap(toRename);
                dm.invalidateFor(td,DependencyManager.RENAME,transactionResource.getLcc());

                //look for foreign key dependency on the column.
                ConstraintDescriptorList constraintDescriptorList=dd.getConstraintDescriptors(td);
                for(int index=0;index<constraintDescriptorList.size();index++){
                    ConstraintDescriptor constraintDescriptor=constraintDescriptorList.elementAt(index);
                    int[] referencedColumns=constraintDescriptor.getReferencedColumns();
                    int numRefCols=referencedColumns.length;
                    for(int j=0;j<numRefCols;j++){
                        if((referencedColumns[j]==columnPosition) &&
                                (constraintDescriptor instanceof ReferencedKeyConstraintDescriptor))
                            dm.invalidateFor(constraintDescriptor,DependencyManager.RENAME,transactionResource.getLcc());
                    }
                }
            }finally{
                if(prepared)
                    transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    public static void preRenameIndex(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException  {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preRenameIndex with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean prepared = false;
            try{
                prepared=transactionResource.marshallTransaction(txn);
                DerbyMessage.UUID uuuid=change.getRenameIndex().getTableId();
                TableDescriptor td=dd.getTableDescriptor(ProtoUtil.getDerbyUUID(uuuid));
                if(td==null) // Table Descriptor transaction never committed
                    return;
                dm.invalidateFor(td,DependencyManager.RENAME_INDEX,transactionResource.getLcc());
            }finally{
                if(prepared)
                    transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    public static void preDropAlias(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException  {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preDropAlias with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean prepared = false;
            try{
                prepared=transactionResource.marshallTransaction(txn);
                DDLMessage.DropAlias dropAlias=change.getDropAlias();
                AliasDescriptor ad=dd.getAliasDescriptor(dropAlias.getSchemaName(),dropAlias.getAliasName(),dropAlias.getNamespace().charAt(0));
                if(ad==null) // Table Descriptor transaction never committed
                    return;
                DropAliasConstantOperation.invalidate(ad,dm,transactionResource.getLcc());
            }finally{
                if(prepared)
                    transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    public static void preNotifyJarLoader(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException  {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preNotifyJarLoader with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean prepared = false;
            try{
                prepared = transactionResource.marshallTransaction(txn);
                dd.invalidateAllSPSPlans(); // This will break other nodes, must do ddl
                ClassFactory cf = transactionResource.getLcc().getLanguageConnectionFactory().getClassFactory();
                cf.notifyModifyJar(change.getNotifyJarLoader().getReload());
                if (change.getNotifyJarLoader().getDrop()) {
                    SchemaDescriptor sd = dd.getSchemaDescriptor(change.getNotifyJarLoader().getSchemaName(), null, true);
                    if (sd ==null)
                        return;
                    FileInfoDescriptor fid = dd.getFileInfoDescriptor(sd,change.getNotifyJarLoader().getSqlName());
                    if (fid==null)
                        return;
                    dm.invalidateFor(fid, DependencyManager.DROP_JAR, transactionResource.getLcc());

                }
            }finally{
                if(prepared)
                    transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    public static void postNotifyJarLoader(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException  {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preNotifyJarLoader with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean prepared = false;
            try{
                prepared = transactionResource.marshallTransaction(txn);
                ClassFactory cf = transactionResource.getLcc().getLanguageConnectionFactory().getClassFactory();
                cf.notifyModifyJar(true);
            }finally{
                if(prepared)
                    transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }

    }

    public static void preNotifyModifyClasspath(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException  {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preDropView with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean prepared = false;
            try{
                prepared = transactionResource.marshallTransaction(txn);
                transactionResource.getLcc().getLanguageConnectionFactory().getClassFactory().notifyModifyClasspath(change.getNotifyModifyClasspath().getClasspath());
            }finally{
                if(prepared)
                    transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }



    public static void preDropView(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException  {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preDropView with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean prepared = false;
            try{
                prepared = transactionResource.marshallTransaction(txn);
                DDLMessage.DropView dropView=change.getDropView();

                TableDescriptor td=dd.getTableDescriptor(ProtoUtil.getDerbyUUID(dropView.getTableId()));
                if(td==null) // Table Descriptor transaction never committed
                    return;
                dm.invalidateFor(td,DependencyManager.DROP_VIEW,transactionResource.getLcc());
            }finally{
                if(prepared)
                    transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    public static void preDropSequence(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException  {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preDropSequence with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean prepared = false;
            try{
                prepared = transactionResource.marshallTransaction(txn);
                DDLMessage.DropSequence dropSequence=change.getDropSequence();
                TransactionController tc = transactionResource.getLcc().getTransactionExecute();
                SchemaDescriptor sd = dd.getSchemaDescriptor(dropSequence.getSchemaName(),tc,true);
                if(sd==null) // Table Descriptor transaction never committed
                    return;
                SequenceDescriptor seqDesc = dd.getSequenceDescriptor(sd,dropSequence.getSequenceName());
                if (seqDesc==null)
                    return;
                dm.invalidateFor(seqDesc, DependencyManager.DROP_SEQUENCE, transactionResource.getLcc());
            }finally{
                if(prepared)
                    transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    public static void preCreateRole(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException  {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preCreateTrigger with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean prepared = false;
            try{
                prepared=transactionResource.marshallTransaction(txn);
                String roleName=change.getCreateRole().getRoleName();
                dd.getDataDictionaryCache().roleCacheRemove(roleName);
            }finally{
                if(prepared)
                    transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }


    public static void preCreateTrigger(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException  {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preCreateTrigger with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean prepared = false;
            try{
                prepared=transactionResource.marshallTransaction(txn);
                DerbyMessage.UUID uuuid=change.getCreateTrigger().getTableId();
                TableDescriptor td=dd.getTableDescriptor(ProtoUtil.getDerbyUUID(uuuid));
                if(td==null)
                    return;
                dm.invalidateFor(td,DependencyManager.CREATE_TRIGGER,transactionResource.getLcc());
            }finally{
                if(prepared)
                    transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    public static void preDropTrigger(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException  {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preDropTrigger with change=%s",change);
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
            boolean prepared = false;
            try{
                prepared = transactionResource.marshallTransaction(txn);
                DerbyMessage.UUID tableuuid=change.getDropTrigger().getTableId();
                DerbyMessage.UUID triggeruuid=change.getDropTrigger().getTriggerId();
                SPSDescriptor spsd=dd.getSPSDescriptor(ProtoUtil.getDerbyUUID(change.getDropTrigger().getSpsDescriptorUUID()));

                TableDescriptor td=dd.getTableDescriptor(ProtoUtil.getDerbyUUID(tableuuid));
                TriggerDescriptor triggerDescriptor=dd.getTriggerDescriptor(ProtoUtil.getDerbyUUID(triggeruuid));
                if(td!=null)
                    dm.invalidateFor(td,DependencyManager.DROP_TRIGGER,transactionResource.getLcc());
                if(triggerDescriptor!=null){
                    dm.invalidateFor(triggerDescriptor,DependencyManager.DROP_TRIGGER,transactionResource.getLcc());
//                dm.clearDependencies(transactionResource.getLcc(), triggerDescriptor);
                    if(triggerDescriptor.getWhenClauseId()!=null){
                        SPSDescriptor whereDescriptor=dd.getSPSDescriptor(triggerDescriptor.getWhenClauseId());
                        if(whereDescriptor!=null){
                            dm.invalidateFor(whereDescriptor,DependencyManager.DROP_TRIGGER,transactionResource.getLcc());
//                        dm.clearDependencies(transactionResource.getLcc(), whereDescriptor);
                        }
                    }
                }
                if(spsd!=null){
                    dm.invalidateFor(spsd,DependencyManager.DROP_TRIGGER,transactionResource.getLcc());
                    //               dm.clearDependencies(transactionResource.getLcc(), spsd);
                }
                // Remove all TECs from trigger stack. They will need to be rebuilt.
                transactionResource.getLcc().popAllTriggerExecutionContexts();
            }finally{
                if(prepared)
                    transactionResource.close();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }



    public static void preAlterTable(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException  {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preAlterTable with change=%s",change);
        boolean prepared = false;
        SpliceTransactionResourceImpl transactionResource = null;
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            transactionResource = new SpliceTransactionResourceImpl();
            prepared = transactionResource.marshallTransaction(txn);
            for (DerbyMessage.UUID uuid : change.getAlterTable().getTableIdList()) {
                TableDescriptor td = dd.getTableDescriptor(ProtoUtil.getDerbyUUID(uuid));
                if (td==null) // Table Descriptor transaction never committed
                    return;
                dm.invalidateFor(td, DependencyManager.ALTER_TABLE, transactionResource.getLcc());
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        } finally {
            if (prepared)
                transactionResource.close();
        }
    }

    public static void preDropRole(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException{
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preDropRole with change=%s",change);
        SpliceTransactionResourceImpl transactionResource = null;
        boolean prepared = false;
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            transactionResource = new SpliceTransactionResourceImpl();
            prepared = transactionResource.marshallTransaction(txn);
            String roleName = change.getDropRole().getRoleName();
            RoleClosureIterator rci =
                dd.createRoleClosureIterator
                    (transactionResource.getLcc().getTransactionCompile(),
                     roleName, false);

            String role;
            while ((role = rci.next()) != null) {
                RoleGrantDescriptor r = dd.getRoleDefinitionDescriptor(role);
                if (r!=null) {
                    dm.invalidateFor(r, DependencyManager.REVOKE_ROLE, transactionResource.getLcc());
                }
            }

            dd.getDataDictionaryCache().roleCacheRemove(change.getDropRole().getRoleName());
            // role grant cache may have entries of this role being granted to others, so need to invalidate
            dd.getDataDictionaryCache().clearRoleGrantCache();
            // permission cache may have permission entries related to this role, so need to invalidate
            dd.getDataDictionaryCache().clearPermissionCache();

        } catch (Exception e) {
            e.printStackTrace();
            throw StandardException.plainWrapException(e);
        } finally {
            if (prepared) {
                transactionResource.close();
            }
        }
    }

    public static void preTruncateTable(DDLMessage.DDLChange change,
                                        DataDictionary dd,
                                        DependencyManager dm) throws StandardException{
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preTruncateTable with change=%s",change);
        boolean prepared = false;
        SpliceTransactionResourceImpl transactionResource = null;
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            transactionResource = new SpliceTransactionResourceImpl();
            //transactionResource.prepareContextManager();
            prepared = transactionResource.marshallTransaction(txn);
            BasicUUID uuid = ProtoUtil.getDerbyUUID(change.getTruncateTable().getTableId());
            TableDescriptor td = dd.getTableDescriptor(uuid);
            dm.invalidateFor(td, DependencyManager.TRUNCATE_TABLE, transactionResource.getLcc());
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        } finally {
            if (prepared)
                transactionResource.close();
        }
    }

    public static void preRevokePrivilege(DDLMessage.DDLChange change,
                                          DataDictionary dd,
                                          DependencyManager dm) throws StandardException{
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preRevokePrivilege with change=%s",change);
        boolean prepared = false;
        SpliceTransactionResourceImpl transactionResource = null;
        try {
            DDLMessage.RevokePrivilege revokePrivilege = change.getRevokePrivilege();
            DDLMessage.RevokePrivilege.Type type = revokePrivilege.getType();
            DDLMessage.RevokePrivilege.OpType op = revokePrivilege.getOp();
            boolean isGrant = (op == DDLMessage.RevokePrivilege.OpType.GRANT_OP);

            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            transactionResource = new SpliceTransactionResourceImpl();
            //transactionResource.prepareContextManager();
            prepared = transactionResource.marshallTransaction(txn);
            LanguageConnectionContext lcc = transactionResource.getLcc();
            if (type == DDLMessage.RevokePrivilege.Type.REVOKE_TABLE_PRIVILEGE) {
                preRevokeTablePrivilege(revokePrivilege.getRevokeTablePrivilege(), dd, dm, lcc, isGrant);
            }
            else if (type == DDLMessage.RevokePrivilege.Type.REVOKE_COLUMN_PRIVILEGE) {
                preRevokeColumnPrivilege(revokePrivilege.getRevokeColumnPrivilege(), dd, dm, lcc, isGrant);
            }
            else if (type == DDLMessage.RevokePrivilege.Type.REVOKE_ROUTINE_PRIVILEGE) {
                preRevokeRoutinePrivilege(revokePrivilege.getRevokeRoutinePrivilege(), dd, dm, lcc, isGrant);
            }
            else if (type == DDLMessage.RevokePrivilege.Type.REVOKE_SCHEMA_PRIVILEGE) {
                preRevokeSchemaPrivilege(revokePrivilege.getRevokeSchemaPrivilege(), dd, dm, lcc, isGrant);
            }
            else if (type == DDLMessage.RevokePrivilege.Type.REVOKE_GENERIC_PRIVILEGE) {
                preRevokeGenericPrivilege(revokePrivilege.getRevokeGenericPrivilege(), dd, dm, lcc, isGrant);
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        } finally {
            if (prepared)
                transactionResource.close();
        }
    }

    private static void preRevokeTablePrivilege(DDLMessage.RevokeTablePrivilege revokeTablePrivilege,
                                                DataDictionary dd,
                                                DependencyManager dm,
                                                LanguageConnectionContext lcc,
                                                boolean isGrant) throws StandardException{

        BasicUUID uuid = ProtoUtil.getDerbyUUID(revokeTablePrivilege.getTableId());
        BasicUUID objectId = ProtoUtil.getDerbyUUID(revokeTablePrivilege.getPermObjectId());
        TableDescriptor td = dd.getTableDescriptor(uuid);

        TablePermsDescriptor tablePermsDesc =
            new TablePermsDescriptor(
                dd,
                revokeTablePrivilege.getGrantee(),
                revokeTablePrivilege.getGrantor(),
                uuid,
                revokeTablePrivilege.getSelectPerm(),
                revokeTablePrivilege.getDeletePerm(),
                revokeTablePrivilege.getInsertPerm(),
                revokeTablePrivilege.getUpdatePerm(),
                revokeTablePrivilege.getReferencesPerm(),
                revokeTablePrivilege.getTriggerPerm());
        tablePermsDesc.setUUID(objectId);
        if (!isGrant) {
            dm.invalidateFor(tablePermsDesc, DependencyManager.REVOKE_PRIVILEGE, lcc);
            dm.invalidateFor(td, DependencyManager.INTERNAL_RECOMPILE_REQUEST, lcc);
        }
        dd.getDataDictionaryCache().permissionCacheRemove(tablePermsDesc);
    }

    private static void preRevokeSchemaPrivilege(DDLMessage.RevokeSchemaPrivilege revokeSchemaPrivilege,
                                                DataDictionary dd,
                                                DependencyManager dm,
                                                LanguageConnectionContext lcc,
                                                boolean isGrant) throws StandardException{

        BasicUUID uuid = ProtoUtil.getDerbyUUID(revokeSchemaPrivilege.getSchemaId());
        BasicUUID objectId = ProtoUtil.getDerbyUUID(revokeSchemaPrivilege.getPermObjectId());
        SchemaDescriptor sd = dd.getSchemaDescriptor(uuid, null);

        SchemaPermsDescriptor schemaPermsDesc =
                new SchemaPermsDescriptor(
                        dd,
                        revokeSchemaPrivilege.getGrantee(),
                        revokeSchemaPrivilege.getGrantor(),
                        uuid,
                        revokeSchemaPrivilege.getSelectPerm(),
                        revokeSchemaPrivilege.getDeletePerm(),
                        revokeSchemaPrivilege.getInsertPerm(),
                        revokeSchemaPrivilege.getUpdatePerm(),
                        revokeSchemaPrivilege.getReferencesPerm(),
                        revokeSchemaPrivilege.getTriggerPerm(),
                        revokeSchemaPrivilege.getModifyPerm(),
                        revokeSchemaPrivilege.getAccessPerm());
        schemaPermsDesc.setUUID(objectId);
        if (!isGrant) {
            dm.invalidateFor(schemaPermsDesc, DependencyManager.REVOKE_PRIVILEGE, lcc);
            dm.invalidateFor(sd, DependencyManager.INTERNAL_RECOMPILE_REQUEST, lcc);
        }
        dd.getDataDictionaryCache().permissionCacheRemove(schemaPermsDesc);
    }

    private static void preRevokeColumnPrivilege(DDLMessage.RevokeColumnPrivilege revokeColumnPrivilege,
                                                 DataDictionary dd,
                                                 DependencyManager dm,
                                                 LanguageConnectionContext lcc,
                                                 boolean isGrant) throws StandardException{

        BasicUUID uuid = ProtoUtil.getDerbyUUID(revokeColumnPrivilege.getTableId());
        BasicUUID objectId = ProtoUtil.getDerbyUUID(revokeColumnPrivilege.getPermObjectId());
        TableDescriptor td = dd.getTableDescriptor(uuid);
        ColPermsDescriptor colPermsDescriptor =
            new ColPermsDescriptor(
                dd,
                revokeColumnPrivilege.getGrantee(),
                revokeColumnPrivilege.getGrantor(),
                uuid,
                revokeColumnPrivilege.getType(),
                revokeColumnPrivilege.hasColumns()?new FormatableBitSet(revokeColumnPrivilege.getColumns().toByteArray()):null);
        colPermsDescriptor.setUUID(objectId);

        if (!isGrant) {
            // only revoke statements need to invalidate the dependent objects
            dm.invalidateFor(colPermsDescriptor, DependencyManager.REVOKE_PRIVILEGE, lcc);
            dm.invalidateFor(td, DependencyManager.INTERNAL_RECOMPILE_REQUEST, lcc);
        }

        // both grant and revoke column permissions need to trigger cache invalidation
        dd.getDataDictionaryCache().permissionCacheRemove(colPermsDescriptor);
    }

    private static void preRevokeRoutinePrivilege(DDLMessage.RevokeRoutinePrivilege revokeRoutinePrivilege,
                                                  DataDictionary dd,
                                                  DependencyManager dm,
                                                  LanguageConnectionContext lcc,
                                                  boolean isGrant) throws StandardException{

        BasicUUID uuid = ProtoUtil.getDerbyUUID(revokeRoutinePrivilege.getRountineId());
        BasicUUID objectId = ProtoUtil.getDerbyUUID(revokeRoutinePrivilege.getPermObjectId());
        RoutinePermsDescriptor routinePermsDescriptor =
            new RoutinePermsDescriptor(
                dd,
                revokeRoutinePrivilege.getGrantee(),
                revokeRoutinePrivilege.getGrantor(),
                uuid);
        routinePermsDescriptor.setUUID(objectId);

        if (!isGrant) {
            dm.invalidateFor(routinePermsDescriptor, DependencyManager.REVOKE_PRIVILEGE_RESTRICT, lcc);

            AliasDescriptor aliasDescriptor = dd.getAliasDescriptor(objectId);
            if (aliasDescriptor != null) {
                dm.invalidateFor(aliasDescriptor, DependencyManager.INTERNAL_RECOMPILE_REQUEST, lcc);
            }
        }
        dd.getDataDictionaryCache().permissionCacheRemove(routinePermsDescriptor);
    }

    private static void preRevokeGenericPrivilege(DDLMessage.RevokeGenericPrivilege revokeGenericPrivilege,
                                                  DataDictionary dd,
                                                  DependencyManager dm,
                                                  LanguageConnectionContext lcc,
                                                  boolean isGrant) throws StandardException{

        BasicUUID uuid = ProtoUtil.getDerbyUUID(revokeGenericPrivilege.getId());
        BasicUUID objectId = ProtoUtil.getDerbyUUID(revokeGenericPrivilege.getPermObjectId());
        PermDescriptor permDescriptor =
            new PermDescriptor(
                dd,
                uuid,
                revokeGenericPrivilege.getObjectType(),
                objectId,
                revokeGenericPrivilege.getPermission(),
                revokeGenericPrivilege.getGrantor(),
                revokeGenericPrivilege.getGrantee(),
                revokeGenericPrivilege.getGrantable());
        if (!isGrant) {
            int invalidationType = revokeGenericPrivilege.getRestrict() ?
                    DependencyManager.REVOKE_PRIVILEGE_RESTRICT : DependencyManager.REVOKE_PRIVILEGE;

            dm.invalidateFor(permDescriptor, invalidationType, lcc);

            PrivilegedSQLObject privilegedSQLObject = null;
            if (revokeGenericPrivilege.getObjectType().compareToIgnoreCase("SEQUENCE") == 0) {
                privilegedSQLObject = dd.getSequenceDescriptor(objectId);
            } else {
                privilegedSQLObject = dd.getAliasDescriptor(objectId);
            }
            if (privilegedSQLObject != null) {
                dm.invalidateFor(privilegedSQLObject, invalidationType, lcc);
            }
        }
        dd.getDataDictionaryCache().permissionCacheRemove(permDescriptor);
    }

    public static void preGrantRevokeRole(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preGrantRevokeRole with change=%s",change);
        SpliceTransactionResourceImpl transactionResource = null;
        boolean prepared = false;
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            transactionResource = new SpliceTransactionResourceImpl();
            prepared = transactionResource.marshallTransaction(txn);
            //remove corresponding defaultRole entry
            dd.getDataDictionaryCache().defaultRoleCacheRemove(change.getGrantRevokeRole().getGranteeName());

            // remove role grant cache
            String roleName = change.getGrantRevokeRole().getRoleName();
            String granteeName = change.getGrantRevokeRole().getGranteeName();
            dd.getDataDictionaryCache().roleGrantCacheRemove(new Pair<>(roleName, granteeName));
        } catch (Exception e) {
            e.printStackTrace();
            throw StandardException.plainWrapException(e);
        } finally {
            if (prepared) {
                transactionResource.close();
            }
        }

    }

    public static void preSetDatabaseProperty(DDLMessage.DDLChange change, DataDictionary dd, DependencyManager dm) throws StandardException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preSetDatabaseProperty with change=%s",change);
        SpliceTransactionResourceImpl transactionResource = null;
        boolean prepared = false;
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            transactionResource = new SpliceTransactionResourceImpl();
            prepared = transactionResource.marshallTransaction(txn);
            //remove corresponding defaultRole entry
            dd.getDataDictionaryCache().propertyCacheRemove(change.getSetDatabaseProperty().getPropertyName());

        } catch (Exception e) {
            e.printStackTrace();
            throw StandardException.plainWrapException(e);
        } finally {
            if (prepared) {
                transactionResource.close();
            }
        }

    }

    public static void preUpdateSystemProcedures(DDLMessage.DDLChange change, DataDictionary dd) throws StandardException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"preUpdateSystemProcedures with change=%s",change);
        SpliceTransactionResourceImpl transactionResource = null;
        boolean prepared = false;
        try {
            TxnView txn = DDLUtils.getLazyTransaction(change.getTxnId());
            transactionResource = new SpliceTransactionResourceImpl();
            prepared = transactionResource.marshallTransaction(txn);
            //clear alias cache
            DataDictionaryCache dc = dd.getDataDictionaryCache();
            dc.clearAliasCache();
            dc.clearStatementCache();
            dc.clearPermissionCache();
            dc.clearSpsNameCache();
            dc.clearStoredPreparedStatementCache();

        } catch (Exception e) {
            e.printStackTrace();
            throw StandardException.plainWrapException(e);
        } finally {
            if (prepared) {
                transactionResource.close();
            }
        }

    }
}
