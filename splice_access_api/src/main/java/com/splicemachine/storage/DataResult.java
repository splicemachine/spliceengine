package com.splicemachine.storage;

import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/15/15
 */
public interface DataResult extends Iterable<DataCell>{

    DataCell commitTimestamp();

    DataCell tombstone();

    DataCell userData();

    DataCell fkCounter();

    int size();

    DataCell latestCell(byte[] family,byte[] qualifier);

    Iterable<DataCell> columnCells(byte[] family,byte[] qualifier);

    byte[] key();

    Map<byte[],byte[]> familyCellMap(byte[] userColumnFamily);

    DataResult getClone();
}
