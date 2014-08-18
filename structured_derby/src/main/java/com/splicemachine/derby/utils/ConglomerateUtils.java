package com.splicemachine.derby.utils;

import com.carrotsearch.hppc.BitSet;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.ZkUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Utilities related to managing DerbyConglomerates
 *
 * @author Scott Fines
 * Created: 2/2/13 10:11 AM
 */
public class ConglomerateUtils extends SpliceConstants {
	public static final String CONGLOMERATE_ATTRIBUTE = "DERBY_CONGLOMERATE";
	private static Logger LOG = Logger.getLogger(ConglomerateUtils.class);

		/**
	 * Reads stored Conglomerate information and returns it as an instance of {@code instanceClass}.
	 *
	 * @param conglomId the id of the conglomerate
	 * @param instanceClass the type to return
	 * @param <T> the type to return
	 * @return an instance of {@code T} which contains the conglomerate information.
	 */
	public static <T> T readConglomerate(long conglomId, Class<T> instanceClass, TxnView txn) throws StandardException {
		SpliceLogUtils.trace(LOG,"readConglomerate {%d}, for instanceClass {%s}",conglomId,instanceClass);
		Preconditions.checkNotNull(txn);
		Preconditions.checkNotNull(conglomId);
		HTableInterface table = null;
		try {
			table = SpliceAccessManager.getHTable(CONGLOMERATE_TABLE_NAME_BYTES);
			Get get = SpliceUtils.createGet(txn, Bytes.toBytes(conglomId));
            get.addColumn(DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES);
            EntryPredicateFilter predicateFilter  = EntryPredicateFilter.emptyPredicate();
            get.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());

			Result result = table.get(get);
			byte[] data = result.getValue(DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES);

            EntryDecoder entryDecoder = new EntryDecoder();
            try{
                if(data!=null) {
                    entryDecoder.set(data);
                    MultiFieldDecoder decoder = entryDecoder.getEntryDecoder();
                    byte[] nextRaw = decoder.decodeNextBytesUnsorted();

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
		} catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG,"readConglomerateException",Exceptions.parseException(e));
		} finally {
			SpliceAccessManager.closeHTableQuietly(table);
		}
		return null;
	}

		private static <T> T readVersioned(byte[] nextRaw,Class<T> instanceClass) throws IOException, StandardException {
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
				ByteArrayInputStream in = new ByteArrayInputStream(nextRaw);
				ObjectInputStream ois = new ObjectInputStream(in);
				Field binField = null;
				Method setMethod = null;
				try{
						binField = ObjectInputStream.class.getDeclaredField("bin");
						binField.setAccessible(true);
						Object bin = binField.get(ois);
						bin.getClass().getDeclaredMethods();
						setMethod = bin.getClass().getDeclaredMethod("setBlockDataMode",boolean.class);
						setMethod.setAccessible(true);
						setMethod.invoke(bin,false);
				} catch (InvocationTargetException e) {
						throw new IOException(e); //shouldn't happen, because nothing goofy is going on
				} catch (NoSuchMethodException e) {
						throw new IOException(e); //shouldn't happen, since we are messing with java.io classes
				} catch (IllegalAccessException e) {
						throw new IOException(e); //shouldn't happen, since we are messing with java.io classes
				} catch (NoSuchFieldException e) {
						throw new IOException(e); //shouldn't happen, since we are messing with java.io classes
				} finally{
						if(binField!=null)
								binField.setAccessible(false);
						if(setMethod!=null)
								setMethod.setAccessible(false);
				}
				ois.readByte();
				ois.readByte();
				ois.readUTF();
				int off = nextRaw.length-in.available();

				//overwrite the serialVersionUID

				Field svuidField = null;
				long val;
				try {
						svuidField = instanceClass.getDeclaredField("serialVersionUID");
						svuidField.setAccessible(true);
						val = ((Long)svuidField.get(null)).longValue();
				} catch (NoSuchFieldException e) {
						throw new IOException("Programmer forgot to state the serialVersionUID on class "+ instanceClass,e); //this can happen, but it's a programmer error
				} catch (IllegalAccessException e) {
						throw new IOException(e); //should never happen, since we dealt with accessibility
				} finally{
						if(svuidField!=null)
								svuidField.setAccessible(false);
				}
				nextRaw[off + 7] = (byte) (val       );
				nextRaw[off + 6] = (byte) (val >>>  8);
				nextRaw[off + 5] = (byte) (val >>> 16);
				nextRaw[off + 4] = (byte) (val >>> 24);
				nextRaw[off + 3] = (byte) (val >>> 32);
				nextRaw[off + 2] = (byte) (val >>> 40);
				nextRaw[off + 1] = (byte) (val >>> 48);
				nextRaw[off    ] = (byte) (val >>> 56);

				return DerbyBytesUtil.fromBytes(nextRaw);
		}

		/**
	 * Stores information about a new conglomerate, specified by {@code conglomId}.
	 *
	 * @param conglomId the conglom id to store information under
	 * @param conglomerate the conglomerate to store
	 * @throws IOException if something goes wrong and the data can't be stored.
	 */
	public static void createConglomerate(long conglomId, Conglomerate conglomerate, Txn txn) throws StandardException {
		createConglomerate(Long.toString(conglomId), conglomId, DerbyBytesUtil.toBytes(conglomerate),txn);
	}

	/**
	 * Stores information about a new conglomerate, specified by {@code tableName}.
	 *
	 * @param tableName the name of the table
	 * @throws IOException if something goes wrong and the data can't be stored.
	 */
	public static void createConglomerate(String tableName, long conglomId, byte[] conglomData, Txn txn) throws StandardException {
		SpliceLogUtils.debug(LOG, "creating Hbase table for conglom {%s} with data {%s}", tableName, conglomData);
		Preconditions.checkNotNull(txn);
		Preconditions.checkNotNull(conglomData);		
		Preconditions.checkNotNull(tableName);
		HBaseAdmin admin = SpliceUtils.getAdmin();
		HTableInterface table = null;
        EntryEncoder entryEncoder = null;
		try{
			HTableDescriptor td = SpliceUtils.generateDefaultSIGovernedTable(tableName);
			admin.createTable(td);
			table = SpliceAccessManager.getHTable(CONGLOMERATE_TABLE_NAME_BYTES);
			Put put = SpliceUtils.createPut(Bytes.toBytes(conglomId), txn);
            BitSet fields = new BitSet();
            fields.set(0);
            entryEncoder = EntryEncoder.create(SpliceDriver.getKryoPool(),1, fields,null,null,null);
            entryEncoder.getEntryEncoder().encodeNextUnsorted(conglomData);
            put.add(DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES, entryEncoder.encode());
			table.put(put);
		} catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG, "Error Creating Conglomerate", Exceptions.parseException(e));
		}finally{
            if(entryEncoder!=null)
                entryEncoder.close();
			SpliceAccessManager.closeHTableQuietly(table);
			Closeables.closeQuietly(admin);
		}
	}

	/**
	 * Update a conglomerate.
	 *
	 * @param conglomerate the new conglomerate information to update
	 * @throws IOException if something goes wrong and the data can't be stored.
	 */
	public static void updateConglomerate(Conglomerate conglomerate, Txn txn) throws StandardException {
		String tableName = Long.toString(conglomerate.getContainerid());
		SpliceLogUtils.debug(LOG, "updating table {%s} in hbase with serialized data {%s}",tableName,conglomerate);
		HTableInterface table = null;
        EntryEncoder entryEncoder = null;
		try{
			table = SpliceAccessManager.getHTable(CONGLOMERATE_TABLE_NAME_BYTES);
			Put put = SpliceUtils.createPut(Bytes.toBytes(conglomerate.getContainerid()), txn);
            BitSet setFields = new BitSet();
            setFields.set(0);
            entryEncoder = EntryEncoder.create(SpliceDriver.getKryoPool(),1,setFields,null,null,null); //no need to set length-delimited, we aren't
            entryEncoder.getEntryEncoder().encodeNextUnsorted(DerbyBytesUtil.toBytes(conglomerate));
			put.add(DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES, entryEncoder.encode());
			table.put(put);
		}catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG, "update Conglomerate Failed", Exceptions.parseException(e));
		}
		finally{
            if(entryEncoder!=null)
                entryEncoder.close();
			SpliceAccessManager.closeHTableQuietly(table);
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
		return ZkUtils.nextSequenceId(zkSpliceConglomerateSequencePath);
	}

	/**
	 * Split a conglomerate. This is an asynchronous operation.
	 *
	 * @param conglomId the conglomerate to split.
	 * @throws IOException if something goes wrong and the split fails
	 * @throws InterruptedException if the split is interrupted.
	 */
	public static void splitConglomerate(long conglomId) throws IOException, InterruptedException {
		HBaseAdmin admin = SpliceUtils.getAdmin();
		admin.split(Bytes.toBytes(Long.toString(conglomId)));
	}

	/**
	 * Synchronously split a conglomerate around a specific row position.
	 *
	 * This method will block until it detects that the split is completed. Unfortunately,
	 * it must block via polling. For a more responsive version, change the setting
	 * "splice.splitWaitInterval" in splice-site.xml.
	 *
	 * @param conglomId the id of the conglomerate to split
	 * @param position the row to split around
	 * @throws IOException if something goes wrong and the split fails
	 * @throws InterruptedException if the split operation is interrupted.
	 */
	public static void splitConglomerate(long conglomId, byte[] position) throws IOException, InterruptedException {
        splitConglomerate(Bytes.toBytes(Long.toString(conglomId)), position, sleepSplitInterval);
	}

    public static void splitConglomerate(byte[] name, byte[] position, long sleepInterval) throws IOException, InterruptedException {
        HBaseAdmin admin = SpliceUtils.getAdmin();
        admin.split(name,position);

        boolean isSplitting=true;
        while(isSplitting){
            isSplitting=false;
            List<HRegionInfo> regions = admin.getTableRegions(name);
            if (regions != null) {
                for(HRegionInfo region:regions){
                    if(region.isSplit()){
                        isSplitting=true;
                        break;
                    }
                }
            } else {
                isSplitting = true;
            }
            Thread.sleep(sleepInterval);
        }
    }

}
