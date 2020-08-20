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

package com.splicemachine.derby.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.ServiceLoader;

import com.carrotsearch.hppc.BitSet;
import org.apache.log4j.Logger;
import splice.com.google.common.base.Preconditions;

import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.configuration.SQLConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.DataGet;
import com.splicemachine.storage.DataPut;
import com.splicemachine.storage.DataResult;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.SpliceLogUtils;

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
     * Reads stored Conglomerate information and returns it as an instance of {@code instanceClass}.
     *
     * @param conglomId     the id of the conglomerate
     * @param instanceClass the type to return
     * @param <T>           the type to return
     * @return an instance of {@code T} which contains the conglomerate information.
     */
    public static <T> T readConglomerate(long conglomId,Class<T> instanceClass,TxnView txn) throws StandardException{
        SpliceLogUtils.trace(LOG,"readConglomerate {%d}, for instanceClass {%s}",conglomId,instanceClass);
        Preconditions.checkNotNull(txn);
        Preconditions.checkNotNull(conglomId);
        SIDriver driver=SIDriver.driver();
        try(Partition partition=driver.getTableFactory().getTable(SQLConfiguration.getConglomerateTableNameBytes())){
            DataGet get=driver.getOperationFactory().newDataGet(txn,Bytes.toBytes(conglomId),null);
            get.returnAllVersions();
            get.addColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES);
            EntryPredicateFilter predicateFilter=EntryPredicateFilter.emptyPredicate();
            get.addAttribute(SIConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());

            DataResult result=partition.get(get,null);
            byte[] data=result.userData().value();

            EntryDecoder entryDecoder=new EntryDecoder();
            try{
                if(data!=null){
                    entryDecoder.set(data);
                    MultiFieldDecoder decoder=entryDecoder.getEntryDecoder();
                    byte[] nextRaw=decoder.decodeNextBytesUnsorted();

                    try{
                        return DerbyBytesUtil.fromBytesUnsafe(nextRaw);
                    }catch(InvalidClassException ice){
                        LOG.error("InvalidClassException detected when reading conglomerate "+conglomId+
                                ". Attempting to resolve the ambiguity in serialVersionUIDs, but serialization errors may result:"+ice.getMessage());
                        return readVersioned(nextRaw,instanceClass);
                    }
                }
            }finally{
                entryDecoder.close();
            }
        }catch(Exception e){
            SpliceLogUtils.logAndThrow(LOG,"readConglomerateException",Exceptions.parseException(e));
        }
        return null;
    }

    private static <T> T readVersioned(byte[] nextRaw,Class<T> instanceClass) throws IOException, StandardException{
                /*
				 * Unfortunately, back in the day we decided to encode Conglomerates using straight Java serialization.
				 * Even more unfortunately, we then forgot to set the serialVersionUID on the Conglomerates that we would
				 * be writing. Even MORE unfortunately than that, we didn't detect the issue until after our first beta
				 * release was exposed, meaning that we couldn't push in a random backwards-incompatible change like setting
				 * the serialVersionUID on these fields without addressing backwards compatibility.
				 *
				 * Because the serialVersionUID wasn't explicitly set early on, different compilers would generate different
				 * default UIDs, which would mean that sometimes upgrades would work, and sometimes they wouldn't, depending
				 * on who compiles it and when and with what version. This is super awkward. The perfect fix is to set
				 * the serialVersionUID explicitely, which will force all different versions to work together (as long as the
				 * data version itself doesn't change).
				 *
				 * However, doing so breaks backwards compatibility, which is awful and horrible and cannot be tolerated.
				 * Therefore, we need to do the following:
				 *
				 * 1. set the serialVersionUID on Conglomerates
				 * 2. NEVER CHANGE THEM or how they serialize/deserialize EVER AGAIN. If we want to change the Conglomerate that
				 * badly, we'll need to create a new class to hold the changes.
				 * 3. avoid the serialVersionUID conflict by somehow hacking around Java's serialization mechanism to be prettier
				 * about it.
				 *
				 * #3 is handled by this method. In essence it will
				 * 1. skip the first few bytes (the stream header, a few type bytes, then the class string)
				 * 2. put the same serialVersionUID in the field as the Conglomerate currently has
				 * 3. re-attempt the read.
				 *
				 * Of course, java.io.* doesn't like us screwing around with their serialization format, so we have to
				 * use Reflection to get access to a few fields and manipulate them. Thus, this is also fragile with
				 * respect to JVM changes--if the Java devs ever change how the ObjectInputStream is implemented, then
				 * we'll be up a creek with a very short paddle.
				 *
				 * Of course, this mechanism will ALSO break if the serialization code ever
				 * changes in a non-backwards-compatible way, so NEVER DO THAT.
				 *
				 * Yes, I realize how horrible, hacky, dangerous, and disgusting this block of code is. If you have
				 * an alternative, feel free. But after you've banged your head for a while on trying to re-implement
				 * ObjectInputStream, you can just deal with the steaming pile of cow-dung which is this particular solution.
				 *
				 * Or you can just hope that this works and never change anything about Conglomerates. Ever. For any reason.
				 * Then you'll be fine.
			 	*/
        ByteArrayInputStream in=new ByteArrayInputStream(nextRaw);
        ObjectInputStream ois=new ObjectInputStream(in);
        Field binField=null;
        Method setMethod=null;
        try{
            binField=ObjectInputStream.class.getDeclaredField("bin");
            binField.setAccessible(true);
            Object bin=binField.get(ois);
            bin.getClass().getDeclaredMethods();
            setMethod=bin.getClass().getDeclaredMethod("setBlockDataMode",boolean.class);
            setMethod.setAccessible(true);
            setMethod.invoke(bin,false);
        }catch(InvocationTargetException|NoSuchFieldException|IllegalAccessException|NoSuchMethodException e){
            throw new IOException(e); //shouldn't happen, because nothing goofy is going on
        }finally{
            if(binField!=null)
                binField.setAccessible(false);
            if(setMethod!=null)
                setMethod.setAccessible(false);
        }
        ois.readByte();
        ois.readByte();
        ois.readUTF();
        int off=nextRaw.length-in.available();

        //overwrite the serialVersionUID

        Field svuidField=null;
        long val;
        try{
            svuidField=instanceClass.getDeclaredField("serialVersionUID");
            svuidField.setAccessible(true);
            val=(Long)svuidField.get(null);
        }catch(NoSuchFieldException e){
            throw new IOException("Programmer forgot to state the serialVersionUID on class "+instanceClass,e); //this can happen, but it's a programmer error
        }catch(IllegalAccessException e){
            throw new IOException(e); //should never happen, since we dealt with accessibility
        }finally{
            if(svuidField!=null)
                svuidField.setAccessible(false);
        }
        nextRaw[off+7]=(byte)(val);
        nextRaw[off+6]=(byte)(val>>>8);
        nextRaw[off+5]=(byte)(val>>>16);
        nextRaw[off+4]=(byte)(val>>>24);
        nextRaw[off+3]=(byte)(val>>>32);
        nextRaw[off+2]=(byte)(val>>>40);
        nextRaw[off+1]=(byte)(val>>>48);
        nextRaw[off]=(byte)(val>>>56);

        return DerbyBytesUtil.fromBytes(nextRaw);
    }

    /**
     * Stores information about a new conglomerate, specified by {@code conglomId}.
     *
     * @param conglomId    the conglom id to store information under
     * @param conglomerate the conglomerate to store
     * @throws com.splicemachine.db.iapi.error.StandardException if something goes wrong and the data can't be stored.
     */
    public static void createConglomerate(boolean isExternal,long conglomId,Conglomerate conglomerate,Txn txn) throws StandardException{
        createConglomerate(isExternal,Long.toString(conglomId),conglomId,DerbyBytesUtil.toBytes(conglomerate),txn,null,null,null,null,-1,null);
    }

    public static void createConglomerate(boolean isExternal,long conglomId,
                                          Conglomerate conglomerate,
                                          Txn txn,
                                          String schemaDisplayName,
                                          String tableDisplayName,
                                          String indexDisplayName,
                                          byte[][] splitKeys) throws StandardException{
        createConglomerate(isExternal,Long.toString(conglomId),conglomId,DerbyBytesUtil.toBytes(conglomerate), txn,
                schemaDisplayName, tableDisplayName,indexDisplayName,null,-1,splitKeys);
    }

    public static void createConglomerate(boolean isExternal,long conglomId,
                                          Conglomerate conglomerate,
                                          TxnView txn,
                                          String schemaDisplayName,
                                          String tableDisplayName,
                                          String indexDisplayName,
                                          String catalogVersion,
                                          long partitionSize,
                                          byte[][] splitKeys) throws StandardException{
        createConglomerate(isExternal,Long.toString(conglomId),conglomId,DerbyBytesUtil.toBytes(conglomerate),txn,
                schemaDisplayName, tableDisplayName,indexDisplayName,catalogVersion,partitionSize, splitKeys);
    }


    public static void markConglomerateDropped(long conglomId, Txn txn) throws StandardException {
        SIDriver driver=SIDriver.driver();
        PartitionFactory tableFactory=driver.getTableFactory();
        try (PartitionAdmin admin = tableFactory.getAdmin()) {
            admin.markDropped(conglomId, txn.getTxnId());
        } catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG, "Error Creating Conglomerate", Exceptions.parseException(e));
        }
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
            TxnView txn,
            String schemaDisplayName,
            String tableDisplayName,
            String indexDisplayName,
            String catalogVersion,
            long partitionSize,
            byte[][] splitKeys) throws StandardException{
        SpliceLogUtils.debug(LOG,"creating Hbase table for conglom {%s} with data {%s}",tableName,conglomData);
        Preconditions.checkNotNull(txn);
        Preconditions.checkNotNull(conglomData);
        Preconditions.checkNotNull(tableName);
        EntryEncoder entryEncoder=null;
        SIDriver driver=SIDriver.driver();
        PartitionFactory tableFactory=driver.getTableFactory();
        if (!isExternal) {
            try (PartitionAdmin admin = tableFactory.getAdmin()) {
                PartitionCreator partitionCreator = admin.newPartition()
                        .withName(tableName)
                        .withDisplayNames(new String[]{schemaDisplayName, tableDisplayName, indexDisplayName})
                        .withTransactionId(txn.getTxnId())
                        .withCatalogVersion(catalogVersion);
                if (partitionSize > 0)
                    partitionCreator = partitionCreator.withPartitionSize(partitionSize);
                if (splitKeys != null && splitKeys.length > 0)
                    partitionCreator = partitionCreator.withSplitKeys(splitKeys);
                partitionCreator.create();
            } catch (Exception e) {
                SpliceLogUtils.logAndThrow(LOG, "Error Creating Conglomerate", Exceptions.parseException(e));
            }
        }

        try(Partition table=tableFactory.getTable(SQLConfiguration.getConglomerateTableNameBytes())){
            DataPut put=driver.getOperationFactory().newDataPut(txn,Bytes.toBytes(conglomId));
            BitSet fields=new BitSet();
            fields.set(0);
            entryEncoder=EntryEncoder.create(SpliceKryoRegistry.getInstance(),1,fields,null,null,null);
            entryEncoder.getEntryEncoder().encodeNextUnsorted(conglomData);
            put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,entryEncoder.encode());
            table.put(put);
        }
        catch(Exception e){
            SpliceLogUtils.logAndThrow(LOG,"Error Creating Conglomerate",Exceptions.parseException(e));
        }finally{
            if(entryEncoder!=null)
                entryEncoder.close();
        }

    }

    /**
     * Update a conglomerate.
     *
     * @param conglomerate the new conglomerate information to update
     * @throws com.splicemachine.db.iapi.error.StandardException if something goes wrong and the data can't be stored.
     */
    public static void updateConglomerate(Conglomerate conglomerate,Txn txn) throws StandardException{
        String tableName=Long.toString(conglomerate.getContainerid());
        SpliceLogUtils.debug(LOG,"updating table {%s} in hbase with serialized data {%s}",tableName,conglomerate);
        EntryEncoder entryEncoder=null;
        SIDriver driver=SIDriver.driver();
        try(Partition table=driver.getTableFactory().getTable(SQLConfiguration.getConglomerateTableNameBytes())){
            DataPut put=driver.getOperationFactory().newDataPut(txn,Bytes.toBytes(conglomerate.getContainerid()));
            BitSet setFields=new BitSet();
            setFields.set(0);
            entryEncoder=EntryEncoder.create(SpliceKryoRegistry.getInstance(),1,setFields,null,null,null); //no need to set length-delimited, we aren't
            entryEncoder.getEntryEncoder().encodeNextUnsorted(DerbyBytesUtil.toBytes(conglomerate));
            put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,entryEncoder.encode());
            table.put(put);
        }catch(Exception e){
            SpliceLogUtils.logAndThrow(LOG,"update Conglomerate Failed",Exceptions.parseException(e));
        }finally{
            if(entryEncoder!=null)
                entryEncoder.close();
        }
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
