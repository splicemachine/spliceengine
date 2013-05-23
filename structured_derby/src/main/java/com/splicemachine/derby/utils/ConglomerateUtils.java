package com.splicemachine.derby.utils;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.ZkUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
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
	public static <T> T readConglomerate(long conglomId, Class<T> instanceClass, String transactionID) throws StandardException {
		SpliceLogUtils.trace(LOG,"readConglomerate {%d}, for instanceClass {%s}",conglomId,instanceClass);
		Preconditions.checkNotNull(transactionID);
		Preconditions.checkNotNull(conglomId);
		HTableInterface table = null;
		try {
			table = SpliceAccessManager.getHTable(CONGLOMERATE_TABLE_NAME_BYTES);
			SpliceUtils.createGet(transactionID, Bytes.toBytes(conglomId));
			Result result = table.get(SpliceUtils.createGet(transactionID, Bytes.toBytes(conglomId)));		
			byte[] data = result.getValue(DEFAULT_FAMILY_BYTES, VALUE_COLUMN);
			if(data!=null) {
				return DerbyBytesUtil.fromBytes(data, instanceClass);
			}
		} catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG,"readConglomerateException",Exceptions.parseException(e));
		} finally {
			SpliceAccessManager.closeHTableQuietly(table);
		}
		return null;
	}

	/**
	 * Stores information about a new conglomerate, specified by {@code conglomId}.
	 *
	 * @param conglomId the conglom id to store information under
	 * @param conglomerate the conglomerate to store
	 * @throws IOException if something goes wrong and the data can't be stored.
	 */
	public static void createConglomerate(long conglomId, Conglomerate conglomerate, String transactionID) throws StandardException {
		createConglomerate(Long.toString(conglomId), conglomId, DerbyBytesUtil.toBytes(conglomerate),transactionID);
	}

	/**
	 * Stores information about a new conglomerate, specified by {@code tableName}.
	 *
	 * @param tableName the name of the table
	 * @param conglomerate the conglomerate to store
	 * @throws IOException if something goes wrong and the data can't be stored.
	 */
	public static void createConglomerate(String tableName, long conglomId, byte[] conglomData, String transactionID) throws StandardException {
		SpliceLogUtils.debug(LOG, "creating Hbase table for conglom {%s} with data {%s}", tableName, conglomData);
		Preconditions.checkNotNull(transactionID);
		Preconditions.checkNotNull(conglomData);		
		Preconditions.checkNotNull(tableName);
		HBaseAdmin admin = SpliceUtils.getAdmin();
		HTableInterface table = null;
		try{
			HTableDescriptor td = SpliceUtils.generateDefaultSIGovernedTable(tableName);
			admin.createTable(td);
			table = SpliceAccessManager.getHTable(CONGLOMERATE_TABLE_NAME_BYTES);
			Put put = SpliceUtils.createPut(Bytes.toBytes(conglomId), transactionID);
			put.add(DEFAULT_FAMILY_BYTES, VALUE_COLUMN, conglomData);
			table.put(put);
		} catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG, "Error Creating Conglomerate", Exceptions.parseException(e));
		}finally{
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
	public static void updateConglomerate(Conglomerate conglomerate, String transactionID) throws StandardException {
		String tableName = Long.toString(conglomerate.getContainerid());
		SpliceLogUtils.debug(LOG, "updating table {%s} in hbase with serialized data {%s}",tableName,conglomerate);
		HTableInterface table = null;
		try{
			table = SpliceAccessManager.getHTable(CONGLOMERATE_TABLE_NAME_BYTES);
			Put put = SpliceUtils.createPut(Bytes.toBytes(conglomerate.getContainerid()), transactionID);
			SpliceUtils.createPut(Bytes.toBytes(conglomerate.getContainerid()), transactionID);
			put.add(DEFAULT_FAMILY_BYTES, VALUE_COLUMN, DerbyBytesUtil.toBytes(conglomerate));
			table.put(put);
		}
		catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG, "update Conglomerate Failed", Exceptions.parseException(e));
		}
		finally{
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
