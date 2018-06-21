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

import com.carrotsearch.hppc.LongOpenHashSet;

import com.google.common.collect.Iterables;
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

    public void vacuumDatabase() throws SQLException{

        //get all the conglomerates from sys.sysconglomerates
        PreparedStatement ps = null;
        ResultSet rs = null;
        LongOpenHashSet activeConglomerates = LongOpenHashSet.newInstance();
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

        //get all the tables from HBaseAdmin
        try {
            long oldestActive = getOldestActiveTransaction();
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
                    if (table.getTransactionId() != null) {
                        TxnView txn = SIDriver.driver().getTxnSupplier().getTransaction(Long.parseLong(table.getTransactionId()));
                        if (txn.getEffectiveBeginTimestamp() >= oldestActive) {
                            // This conglomerate was created by a "recent" transaction (newer than the oldest active txn)
                            // ignore it in case it's still in use

                            if (LOG.isInfoEnabled()) {
                                LOG.info("Ignoring recently created table: " + table.getTableName() + " by transaction " + txn.getTxnId());
                            }
                            continue;
                        }
                    }
                    if(!activeConglomerates.contains(tableConglom)){
                        LOG.info("Deleting inactive table: " + table.getTableName());
                        partitionAdmin.deleteTable(tableName[1]);
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
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(e);
        } catch (IOException e) {
            LOG.error("Vacuum Unexpected exception", e);
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        } finally {
            LOG.info("Vacuum complete");
        }
    }

    private long getOldestActiveTransaction() throws StandardException {
        EmbedConnection embedConnection = (EmbedConnection)connection;

        TransactionController transactionExecute = embedConnection.getLanguageConnection().getTransactionExecute();
        TxnView txn = ((SpliceTransactionManager) transactionExecute).getActiveStateTxn();

        ActiveTransactionReader reader = new ActiveTransactionReader(0l,txn.getTxnId(),null);

        long activeTxn;

        try {
            activeTxn = txn.getEffectiveBeginTimestamp();

            TxnView next;
            try (Stream<TxnView> activeTransactions = reader.getActiveTransactions()){
                while((next = activeTransactions.next())!=null){
                    long txnId = next.getEffectiveBeginTimestamp();
                    if(txnId < activeTxn){
                        activeTxn = txnId;
                        break;
                    }
                }
            }

            return activeTxn;
        } catch (IOException | StreamException e) {
            throw Exceptions.parseException(e);
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
