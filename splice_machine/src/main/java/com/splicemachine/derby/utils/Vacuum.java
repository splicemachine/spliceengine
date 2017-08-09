/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.carrotsearch.hppc.LongOpenHashSet;

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
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.StreamException;

/**
 * Utility for Vacuuming Splice.
 *
 * @author Scott Fines
 *         Date: 3/19/14
 */
public class Vacuum{

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

        ensurePriorTransactionsComplete();

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

        //get all the tables from HBaseAdmin
        try {
            Iterable<TableDescriptor> hTableDescriptors = partitionAdmin.listTables();

            for(TableDescriptor table:hTableDescriptors){
                try{
                    String[] tableName = parseTableName(table.getTableName());
                    if (tableName.length < 2) return;
                    long tableConglom = Long.parseLong(tableName[1]);
                    if(tableConglom < DataDictionary.FIRST_USER_TABLE_NUMBER) continue; //ignore system tables
                    if(!activeConglomerates.contains(tableConglom)){
                        partitionAdmin.deleteTable(tableName[1]);
                    }
                }catch(NumberFormatException nfe){
                    /*This is either TEMP, TRANSACTIONS, SEQUENCES, or something
					 * that's not managed by splice. Ignore it
					 */
                }
            }
        } catch (IOException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

    /*
     * We have to make sure that all prior transactions complete. Once that happens, we know that the worldview
     * of all outstanding transactions is the same as ours--so if a conglomerate doesn't exist in sysconglomerates,
     * then it's not useful anymore.
     */
    private void ensurePriorTransactionsComplete() throws SQLException {
        EmbedConnection embedConnection = (EmbedConnection)connection;

        TransactionController transactionExecute = embedConnection.getLanguageConnection().getTransactionExecute();
        Txn activeStateTxn = ((SpliceTransactionManager) transactionExecute).getActiveStateTxn();

        //wait for all transactions prior to us to complete, but only wait for so long
        try{
            long activeTxn = waitForConcurrentTransactions(activeStateTxn);
            if(activeTxn>0){
                //we can't do anything, blow up
                throw PublicAPI.wrapStandardException(
                        ErrorState.DDL_ACTIVE_TRANSACTIONS.newException("VACUUM", activeTxn));
            }

        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    private long waitForConcurrentTransactions(Txn txn) throws StandardException {
        /*
        ActiveTransactionReader reader = new ActiveTransactionReader(0l,txn.getTxnId(),null);
        SConfiguration config = EngineDriver.driver().getConfiguration();
        long timeRemaining = config.getDdlDrainingMaximumWait();
        long pollPeriod = config.getDdlDrainingInitialWait();
        int tryNum = 1;
        long activeTxn;

        try {
            do {
                activeTxn = -1l;

                Txn next;
                try (Stream<TxnView> activeTransactions = reader.getActiveTransactions()){
                    while((next = activeTransactions.next())!=null){
                        long txnId = next.getTxnId();
                        if(txnId!=txn.getTxnId()){
                            activeTxn = txnId;
                            break;
                        }
                    }
                }

                if(activeTxn<0) return activeTxn; //no active transactions

                long time = System.currentTimeMillis();

                try {
                    Thread.sleep(Math.min(tryNum*pollPeriod,timeRemaining));
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
                timeRemaining-=(System.currentTimeMillis()-time);
                tryNum++;
            } while (timeRemaining>0);
        } catch (IOException | StreamException e) {
            throw Exceptions.parseException(e);
        }

        return activeTxn;
        */
        throw new UnsupportedOperationException();
    } // end waitForConcurrentTransactions

    public void shutdown() throws SQLException {
        try {
            partitionAdmin.close();
        } catch (IOException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

    private String[] parseTableName(String name) {
        String[] tableName = name.split(":");
        return tableName;
    }
}
