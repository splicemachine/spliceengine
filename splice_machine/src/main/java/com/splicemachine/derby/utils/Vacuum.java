/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.carrotsearch.hppc.LongHashSet;

import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataDelete;
import com.splicemachine.storage.DataScanner;
import com.splicemachine.storage.Partition;
import org.spark_project.guava.collect.Iterables;
import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.TableDescriptor;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.derby.impl.sql.execute.actions.ActiveTransactionReader;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.StreamException;
import org.apache.log4j.Logger;

/**
 * Utility for Vacuuming Splice.
 *
 * @author Scott Fines
 *         Date: 3/19/14
 */
public class Vacuum{
    private static final Logger LOG = Logger.getLogger(Vacuum.class);

    private final Connection connection;
    private final PartitionAdmin partitionAdmin;

    public Vacuum(Connection connection) throws SQLException {
        this.connection = connection;
        try {
            SIDriver driver=SIDriver.driver();
            PartitionFactory partitionFactory = driver.getTableFactory();
            partitionAdmin = partitionFactory.getAdmin();
        } catch (Exception e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

    public void vacuumDatabase(long oldestActiveTransaction) throws SQLException{
        //get all the conglomerates from sys.sysconglomerates
        PreparedStatement ps = null;
        ResultSet rs = null;
        LongHashSet activeConglomerates = new LongHashSet();
        try{
            ps = connection.prepareStatement("select conglomeratenumber from sys.sysconglomerates");
            rs = ps.executeQuery();

            while(rs.next()){
                activeConglomerates.add(rs.getLong(1));
            }
        }finally{
            if(rs!=null)
                rs.close();
            if(ps!=null)
                ps.close();
        }
        LOG.info("Found " + activeConglomerates.size() + " active conglomerates.");

        //get all dropped conglomerates
        Map<Long, Long> droppedConglomerates = new HashMap<>();
        List<Long> toDelete = new ArrayList<>();
        SIDriver driver = SIDriver.driver();
        try (Partition dropped = driver.getTableFactory().getTable(HBaseConfiguration.DROPPED_CONGLOMERATES_TABLE_NAME)) {
            try (DataScanner scanner = dropped.openScanner(driver.baseOperationFactory().newScan())) {
                while (true) {
                    List<DataCell> res = scanner.next(0);
                    if (res.isEmpty())
                        break;
                    for (DataCell dc : res) {
                        long conglomerateId = Bytes.toLong(dc.key());
                        long txnId = Bytes.toLong(dc.value());

                        droppedConglomerates.putIfAbsent(conglomerateId, txnId);
                    }
                }
            }

            //get all the tables from HBaseAdmin
            Iterable<TableDescriptor> hTableDescriptors = partitionAdmin.listTables();

            if (LOG.isTraceEnabled()) {
                LOG.trace("Found " + Iterables.size(hTableDescriptors) + " HBase tables.");
            }

            for(TableDescriptor table:hTableDescriptors){
                try{
                    String[] tableName = parseTableName(table.getTableName());
                    if (tableName.length < 2) {
                        LOG.warn("Table name doesn't have two components (namespace : name) ignoring: " + table.getTableName());
                        continue;
                    }
                    long tableConglom = Long.parseLong(tableName[1]);
                    if(tableConglom < DataDictionary.FIRST_USER_TABLE_NUMBER) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Ignoring system table: " + table.getTableName());
                        }
                        continue; //ignore system tables
                    }
                    boolean requiresDroppedId = false;
                    boolean ignoreDroppedId = false;
                    if (table.getTransactionId() != null) {
                        TxnView txn = SIDriver.driver().getTxnSupplier().getTransaction(Long.parseLong(table.getTransactionId()));
                        if (txn.getEffectiveBeginTimestamp() >= oldestActiveTransaction) {
                            // This conglomerate was created by a "recent" transaction (newer than the oldest active txn)
                            // ignore it in case it's still in use

                            if (LOG.isInfoEnabled()) {
                                LOG.info("Ignoring recently created table: " + table.getTableName() + " by transaction " + txn.getTxnId());
                            }
                            continue;
                        }
                        if (txn.getEffectiveState().equals(Txn.State.ROLLEDBACK)) {
                            // Transaction is rolled back, we can remove it safely, don't pay any mind to the droppedId
                            ignoreDroppedId = true;
                        } else {
                            // This conglomerate requires a dropped transaction id
                            requiresDroppedId = true;
                        }
                    }
                    boolean hasDroppedId = table.getDroppedTransactionId() != null || droppedConglomerates.containsKey(tableConglom);
                    if (!ignoreDroppedId && hasDroppedId) {
                        long txnId;
                        // The first case deals with conglomerates dropped before DB-7501 got merged
                        if (table.getDroppedTransactionId() != null) 
                            txnId = Long.parseLong(table.getDroppedTransactionId());
                        else
                            txnId = droppedConglomerates.get(tableConglom);

                        TxnView txn = SIDriver.driver().getTxnSupplier().getTransaction(txnId);
                        if (txn.getEffectiveCommitTimestamp() == -1 || txn.getEffectiveCommitTimestamp() >= oldestActiveTransaction) {
                            // This conglomerate was dropped by an active, rolled back or "recent" transaction
                            // (newer than the oldest active txn) ignore it in case it's still in use

                            if (LOG.isInfoEnabled()) {
                                LOG.info("Ignoring recently dropped table: " + table.getTableName() + " by transaction " + txn.getTxnId());
                            }
                            continue;
                        }
                    } else if (requiresDroppedId) {
                        // This table must have a dropped transaction id before we can process it for vacuum
                        continue;
                    }
                    if(!activeConglomerates.contains(tableConglom)){
                        LOG.info("Deleting inactive table: " + table.getTableName());
                        partitionAdmin.deleteTable(tableName[1]);
                        toDelete.add(tableConglom);
                    } else if(LOG.isTraceEnabled()) {
                        LOG.trace("Skipping still active table: " + table.getTableName());
                    }
                }catch(NumberFormatException nfe){
                    /*This is either TEMP, TRANSACTIONS, SEQUENCES, or something
					 * that's not managed by splice. Ignore it
					 */
                    LOG.info("Ignoring non-numeric table name: " + table.getTableName());
                }
            }

            List<DataDelete> deletes = new ArrayList<>();
            for (long removed : toDelete) {
                DataDelete delete = driver.baseOperationFactory().newDelete(Bytes.toBytes(removed));
                deletes.add(delete);
            }
            dropped.delete(deletes);
        } catch (IOException e) {
            LOG.error("Vacuum Unexpected exception", e);
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        } finally {
            LOG.info("Vacuum complete");
        }
    }

    public void shutdown() throws SQLException {
        try {
            partitionAdmin.close();
        } catch (IOException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

    private String[] parseTableName(String name) {
        return name.split(":");
    }
}
