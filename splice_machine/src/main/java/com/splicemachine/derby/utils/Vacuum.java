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

import com.carrotsearch.hppc.LongOpenHashSet;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.TableDescriptor;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.impl.driver.SIDriver;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
        //get all the conglomerates from sys.sysconglomerates
        LongOpenHashSet activeConglomerates = LongOpenHashSet.newInstance();
        try(PreparedStatement ps = connection.prepareStatement("select conglomeratenumber from sys.sysconglomerates")){
            try(ResultSet rs = ps.executeQuery()){
                while(rs.next()){
                    activeConglomerates.add(rs.getLong(1));
                }
            }
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
