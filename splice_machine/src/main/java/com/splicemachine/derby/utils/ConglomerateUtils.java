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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.ServiceLoader;
import com.carrotsearch.hppc.BitSet;
import com.splicemachine.si.api.txn.IsolationLevel;
import com.splicemachine.storage.Record;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Preconditions;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.configuration.SQLConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.SpliceLogUtils;

import javax.ws.rs.NotSupportedException;

/**
 * Utilities related to managing DerbyConglomerates
 *
 * @author Scott Fines
 *         Created: 2/2/13 10:11 AM
 */
public class ConglomerateUtils{
    //    public static final String CONGLOMERATE_ATTRIBUTE = "DERBY_CONGLOMERATE";
    private static Logger LOG=Logger.getLogger(ConglomerateUtils.class);

    /**
     * Reads stored Record information and returns it as an instance of {@code instanceClass}.
     *
     * @param conglomId     the id of the conglomerate
     * @param instanceClass the type to return
     * @param <T>           the type to return
     * @return an instance of {@code T} which contains the conglomerate information.
     */
    public static <T> T readConglomerate(long conglomId,Class<T> instanceClass,Txn txn) throws StandardException{
        SpliceLogUtils.trace(LOG,"readConglomerate {%d}, for instanceClass {%s}",conglomId,instanceClass);
        Preconditions.checkNotNull(txn);
        Preconditions.checkNotNull(conglomId);
        SIDriver driver=SIDriver.driver();
        try(Partition partition=driver.getTableFactory().getTable(SQLConfiguration.CONGLOMERATE_TABLE_NAME_BYTES)){
            Record record = partition.get(Bytes.toBytes(conglomId),txn, IsolationLevel.SNAPSHOT_ISOLATION);
            throw new NotSupportedException("not implemented");
        }catch(Exception e){
            SpliceLogUtils.logAndThrow(LOG,"readConglomerateException",Exceptions.parseException(e));
        }
        return null;
    }

    /**
     * Stores information about a new conglomerate, specified by {@code conglomId}.
     *
     * @param conglomId    the conglom id to store information under
     * @param conglomerate the conglomerate to store
     * @throws com.splicemachine.db.iapi.error.StandardException if something goes wrong and the data can't be stored.
     */
    public static void createConglomerate(boolean isExternal,long conglomId,Conglomerate conglomerate,Txn txn) throws StandardException{
        createConglomerate(isExternal,Long.toString(conglomId),conglomId,DerbyBytesUtil.toBytes(conglomerate),txn,null,null,-1);
    }

    public static void createConglomerate(boolean isExternal,long conglomId,
                                          Conglomerate conglomerate,
                                          Txn txn,
                                          String tableDisplayName,
                                          String indexDisplayName) throws StandardException{
        createConglomerate(isExternal,Long.toString(conglomId),conglomId,DerbyBytesUtil.toBytes(conglomerate),txn,tableDisplayName,indexDisplayName,-1);
    }

    public static void createConglomerate(boolean isExternal,long conglomId,
                                          Conglomerate conglomerate,
                                          Txn txn,
                                          String tableDisplayName,
                                          String indexDisplayName,
                                          long partitionSize) throws StandardException{
        createConglomerate(isExternal,Long.toString(conglomId),conglomId,DerbyBytesUtil.toBytes(conglomerate),txn,tableDisplayName,indexDisplayName,partitionSize);
    }

    /**
     * Stores information about a new conglomerate, specified by {@code tableName}.
     *
     * @param tableName the name of the table
     * @throws com.splicemachine.db.iapi.error.StandardException if something goes wrong and the data can't be stored.
     */
    public static void createConglomerate(
            boolean isExternal,
            String tableName,
            long conglomId,
            byte[] conglomData,
            Txn txn,
            String tableDisplayName,
            String indexDisplayName,
            long partitionSize) throws StandardException{
        SpliceLogUtils.debug(LOG,"creating Hbase table for conglom {%s} with data {%s}",tableName,conglomData);
        Preconditions.checkNotNull(txn);
        Preconditions.checkNotNull(conglomData);
        Preconditions.checkNotNull(tableName);
        SIDriver driver=SIDriver.driver();
        PartitionFactory tableFactory=driver.getTableFactory();
        if (!isExternal) {
            try (PartitionAdmin admin = tableFactory.getAdmin()) {
                PartitionCreator partitionCreator = admin.newPartition().withName(tableName).withDisplayNames(new String[]{tableDisplayName, indexDisplayName});
                if (partitionSize > 0)
                    partitionCreator = partitionCreator.withPartitionSize(partitionSize);
                partitionCreator.create();
            } catch (Exception e) {
                SpliceLogUtils.logAndThrow(LOG, "Error Creating Record", Exceptions.parseException(e));
            }
        }

        throw new NotSupportedException("not implemented");
        /*
        try(Partition table=tableFactory.getTable(SQLConfiguration.CONGLOMERATE_TABLE_NAME_BYTES)){
            DataPut put=driver.getOperationFactory().newDataPut(txn,Bytes.toBytes(conglomId));
            BitSet fields=new BitSet();
            fields.set(0);
            entryEncoder=EntryEncoder.create(SpliceKryoRegistry.getInstance(),1,fields,null,null,null);
            entryEncoder.getEntryEncoder().encodeNextUnsorted(conglomData);
            put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,entryEncoder.encode());
            table.put(put);
        }
        catch(Exception e){
            SpliceLogUtils.logAndThrow(LOG,"Error Creating Record",Exceptions.parseException(e));
        }finally{
            if(entryEncoder!=null)
                entryEncoder.close();
        }
        */

    }

    /**
     * Update a conglomerate.
     *
     * @param conglomerate the new conglomerate information to update
     * @throws com.splicemachine.db.iapi.error.StandardException if something goes wrong and the data can't be stored.
     */
    public static void updateConglomerate(Conglomerate conglomerate,Txn txn) throws StandardException{
        throw new NotSupportedException("not implemented");
        /*
        String tableName=Long.toString(conglomerate.getContainerid());
        SpliceLogUtils.debug(LOG,"updating table {%s} in hbase with serialized data {%s}",tableName,conglomerate);
        EntryEncoder entryEncoder=null;
        SIDriver driver=SIDriver.driver();
        try(Partition table=driver.getTableFactory().getTable(SQLConfiguration.CONGLOMERATE_TABLE_NAME_BYTES)){
            DataPut put=driver.getOperationFactory().newDataPut(txn,Bytes.toBytes(conglomerate.getContainerid()));
            BitSet setFields=new BitSet();
            setFields.set(0);
            entryEncoder=EntryEncoder.create(SpliceKryoRegistry.getInstance(),1,setFields,null,null,null); //no need to set length-delimited, we aren't
            entryEncoder.getEntryEncoder().encodeNextUnsorted(DerbyBytesUtil.toBytes(conglomerate));
            put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,entryEncoder.encode());
            table.put(put);
        }catch(Exception e){
            SpliceLogUtils.logAndThrow(LOG,"update Record Failed",Exceptions.parseException(e));
        }finally{
            if(entryEncoder!=null)
                entryEncoder.close();
        }
        */
    }

    /**
     * Gets the next conglomerate id for the system.
     *
     * @return the next conglomerate id for the system.
     * @throws IOException if something goes wrong and the next id can't be fetched.
     */
    public static long getNextConglomerateId() throws IOException{
        LOG.trace("getting next conglomerate id");
        return conglomSequencer().next();
    }

    public static void setNextConglomerateId(long conglomerateId) throws IOException{
        LOG.trace("setting next conglomerate id");
        conglomSequencer().setPosition(conglomerateId);
    }

    private static volatile Sequencer CONGLOM_SEQUENCE;

    private static Sequencer conglomSequencer(){
        Sequencer s=CONGLOM_SEQUENCE;
        if(s==null){
            s=loadConglomSequencer();
        }
        return s;
    }

    private static synchronized Sequencer loadConglomSequencer(){
        Sequencer s=CONGLOM_SEQUENCE;
        if(s==null){
            ServiceLoader<Sequencer> loader=ServiceLoader.load(Sequencer.class);
            Iterator<Sequencer> iter=loader.iterator();
            if(!iter.hasNext())
                throw new IllegalStateException("No Sequencers found!");
            s=CONGLOM_SEQUENCE=iter.next();
        }
        return s;
    }

    public static int[] dropValueFromArray(int[] initialArray,int position){
        if(initialArray.length==1){
            assert position==1: "Position not correct";
            return new int[]{};
        }

        int[] droppedArray=new int[initialArray.length-1];
        System.arraycopy(initialArray,0,droppedArray,0,position);
        if(position!=initialArray.length)
            System.arraycopy(initialArray,position+1,droppedArray,position,initialArray.length-1-position);
        return droppedArray;
    }

}
