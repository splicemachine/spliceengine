package com.splicemachine.si.impl;

import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Map;


/**
 * Library of functions used by the SI module when accessing rows from data tables (data tables as opposed to the
 * transaction table).
 */

@SuppressFBWarnings("EI_EXPOSE_REP2")
public class DataStore<OperationWithAttributes,Data, Get extends OperationWithAttributes, Scan extends OperationWithAttributes> {

    public final SDataLib<OperationWithAttributes,Data, Get,
            Scan> dataLib;
    private final String siNeededAttribute;
    private final String deletePutAttribute;
    private final byte[] commitTimestampQualifier;
    private final byte[] tombstoneQualifier;
    private final byte[] siNull;
    private final byte[] siAntiTombstoneValue;
    private final byte[] userColumnFamily;

    public DataStore(SDataLib dataLib,
                     String siNeededAttribute,
                     String deletePutAttribute,
                     byte[] siCommitQualifier,
                     byte[] siTombstoneQualifier,
                     byte[] siNull,
                     byte[] siAntiTombstoneValue,
                     byte[] userColumnFamily) {
        this.dataLib = dataLib;
        this.siNeededAttribute = siNeededAttribute;
        this.deletePutAttribute = deletePutAttribute;
        this.commitTimestampQualifier = siCommitQualifier;
        this.tombstoneQualifier = siTombstoneQualifier;
        this.siNull = siNull;
        this.siAntiTombstoneValue = siAntiTombstoneValue;
        this.userColumnFamily = userColumnFamily;
    }

    public byte[] getSINeededAttribute(OperationWithAttributes operation) {
        return dataLib.getAttribute(operation,siNeededAttribute);
    }

    public byte[] getSINeededAttribute(Attributable operation) {
        return operation.getAttribute(siNeededAttribute);
    }


    public boolean getDeletePutAttribute(Attributable operation) {
        byte[] neededValue = operation.getAttribute(deletePutAttribute);
        if (neededValue == null) return false;
        return dataLib.decode(neededValue, Boolean.class);
    }

    public CellType getKeyValueType(Data keyValue) {
        if (dataLib.singleMatchingQualifier(keyValue, commitTimestampQualifier)) {
            return CellType.COMMIT_TIMESTAMP;
        } else if (dataLib.singleMatchingQualifier(keyValue, SIConstants.PACKED_COLUMN_BYTES)) {
            return CellType.USER_DATA;
        } else if (dataLib.singleMatchingQualifier(keyValue, tombstoneQualifier)) {
            if (dataLib.matchingValue(keyValue, siNull)) {
                return CellType.TOMBSTONE;
            } else if (dataLib.matchingValue(keyValue, siAntiTombstoneValue)) {
                return CellType.ANTI_TOMBSTONE;
            }
        } else if (dataLib.singleMatchingQualifier(keyValue, SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES)) {
            return CellType.FOREIGN_KEY_COUNTER;
        }
        return CellType.OTHER;
    }

    public void setTombstonesOnColumns(Partition table, long timestamp, DataPut put) throws IOException {
        //-sf- this doesn't really happen in practice, it's just for a safety valve, which is good, cause it's expensive
        final Map<byte[], byte[]> userData = getUserData(table,put.key());
        if (userData != null) {
            for (byte[] qualifier : userData.keySet()) {
                put.addCell(userColumnFamily,qualifier,timestamp,siNull);
            }
        }
    }

    private Map<byte[], byte[]> getUserData(Partition table, byte[] rowKey) throws IOException {
        DataResult dr = table.getLatest(rowKey,userColumnFamily,null);
        if (dr != null) {
            return dr.familyCellMap(userColumnFamily);
        }
        return null;
    }

    public String getTableName(Partition table) {
        return table.getName();
    }

    public SDataLib<OperationWithAttributes,Data, Get,
            Scan> getDataLib() {
        return this.dataLib;
    }
}
