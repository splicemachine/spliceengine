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
 *
 */
package com.splicemachine.vacuum;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.TxnRegistryWatcher;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 11/30/16
 */
public class VacuumAdmin{

    public static void VACUUM_TABLE(String schemaName, String tableName) throws SQLException{
        if(schemaName==null)
            schemaName =EngineUtils.getCurrentSchema();
        else schemaName = EngineUtils.validateSchema(schemaName);

        if(tableName==null)
            throw PublicAPI.wrapStandardException(ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException());
        else tableName = EngineUtils.validateTable(tableName);

        try(Connection internalConn =DriverManager.getConnection("jdbc:default:connection")){
            EmbedConnection embedConn = (EmbedConnection)internalConn;
            LanguageConnectionContext lcc=embedConn.getLanguageConnection();
            TransactionController tc=lcc.getTransactionExecute();
            long myBeginTxn = ((SpliceTransactionManager)tc).getRawTransaction().getActiveStateTxn().getBeginTimestamp();

            TxnRegistryWatcher registryWatcher=EngineDriver.driver()
                    .dbAdministrator()
                    .getGlobalTransactionRegistryWatcher();

//            waitForMat(myBeginTxn,registryWatcher);

            SchemaDescriptor schemaDescriptor=lcc.getDataDictionary().getSchemaDescriptor(schemaName,tc,true);
            TableDescriptor td = lcc.getDataDictionary().getTableDescriptor(tableName,schemaDescriptor,tc);
            vacuumTable(td);
        }catch(StandardException e){
            throw PublicAPI.wrapStandardException(e);
        }
    }

    public static void VACUUM_SCHEMA(String schemaName) throws SQLException{
        if(schemaName==null)
            schemaName =EngineUtils.getCurrentSchema();
        else schemaName = EngineUtils.validateSchema(schemaName);

        try(Connection internalConn =DriverManager.getConnection("jdbc:default:connection")){
            EmbedConnection embedConn = (EmbedConnection)internalConn;
            LanguageConnectionContext lcc=embedConn.getLanguageConnection();
            TransactionController tc=lcc.getTransactionExecute();
            long myBeginTxn = ((SpliceTransactionManager)tc).getRawTransaction().getActiveStateTxn().getBeginTimestamp();

            TxnRegistryWatcher registryWatcher=EngineDriver.driver()
                    .dbAdministrator()
                    .getGlobalTransactionRegistryWatcher();

//            waitForMat(myBeginTxn,registryWatcher);

            SchemaDescriptor schemaDescriptor=lcc.getDataDictionary().getSchemaDescriptor(schemaName,tc,true);
            List<TableDescriptor> alltables = EngineUtils.getAllTableDescriptors(schemaDescriptor,embedConn);
            for(TableDescriptor td:alltables){
                vacuumTable(td);
            }
        }catch(StandardException e){
            throw PublicAPI.wrapStandardException(e);
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static void waitForMat(long myBeginTxn,TxnRegistryWatcher registryWatcher) throws StandardException{
        SConfiguration config =EngineDriver.driver().getConfiguration();
        long waitTime = config.getDdlDrainingInitialWait();
        long maxWait = config.getDdlDrainingMaximumWait();
        long scale = 2; //the scale factor for the exponential backoff
        long timeAvailable = maxWait;
        long currentMat;
        try{
            do{
                currentMat = registryWatcher.currentView(true).getMinimumActiveTransactionId();
                if(currentMat==myBeginTxn) {
                    /*
                     * The mat cannot advance past our transaction (since it will remain
                     * open while we are working), so if the current MAT is the same as our transaction,
                     * then we are in a position where we can advance
                     */
                    return;
                }
                long start = System.currentTimeMillis();
                try{
                    Thread.sleep(waitTime);
                }catch(InterruptedException ie){
                    Thread.currentThread().interrupt();
                    throw Exceptions.parseException(ie);
                }
                long stop = System.currentTimeMillis();
                timeAvailable-=(stop-start);
                waitTime = Math.min(timeAvailable,scale*waitTime);
            } while(timeAvailable>0);

            if(timeAvailable<=0){
                /*
                 * We waited for the MAT to advance for the maximum amount of time and did not
                 * manage to finish off, so we have to bail
                 */
                throw ErrorState.DDL_ACTIVE_TRANSACTIONS.newException("Vacuum",currentMat);
            }
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
    }

    private static void vacuumTable(TableDescriptor td) throws StandardException{
        ConglomerateDescriptor[] conglomerateDescriptors=td.getConglomerateDescriptors();
        PartitionFactory tableFactory =SIDriver.driver().getTableFactory();
        for(ConglomerateDescriptor cd:conglomerateDescriptors){
            long conglomId = cd.getConglomerateNumber();
            try(Partition table = tableFactory.getTable(Long.toString(conglomId))){
                table.flush();
                table.compact();
            }catch(IOException e){
                throw Exceptions.parseException(e);
            }
        }
    }

}
