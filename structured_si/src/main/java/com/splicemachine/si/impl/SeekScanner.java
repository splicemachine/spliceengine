package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Handles the logic of seeking forward to a given row in an underlying scanner and reading that row (if it exists).
 */
public class SeekScanner<IHTable, Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus, Scanner, Hashable extends Comparable> {
    private final Scanner scanner;
    private final SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus> dataLib;
    private final STableReader<IHTable, Result, Get, Scan, KeyValue, Scanner, Data> reader;
    private List<KeyValue> next;
    private final Hasher<Data, Hashable> hasher;

    public SeekScanner(STableReader<IHTable, Result, Get, Scan, KeyValue, Scanner, Data> reader,
                       SDataLib<Data, Result, KeyValue, OperationWithAttributes, Put, Delete, Get, Scan, Lock, OperationStatus> dataLib,
                       Hasher<Data, Hashable> hasher, Scanner scanner) {
        this.reader = reader;
        this.dataLib = dataLib;
        this.hasher = hasher;
        this.scanner = scanner;
    }

    /**
     * Move forward in the underlying scanner to the given rowKey. NOTE: This can only be used for moving forward, so the
     * target rowKey is expected to be "greater" than the current position of the scanner.
     * @return the KeyValues for the specified rowKey. This will be an empty list if the row is not present in the scanner.
     * @throws IOException
     */
    public List<KeyValue> seekRow(Data rowKey) throws IOException {
        while (true) {
            if (next == null) {
                next = reader.nextResultsOnRegionScanner(scanner);
            }
            if (next == null || next.isEmpty()) {
                next = null;
                return Collections.emptyList();
            }
            final Data resultKey = dataLib.getKeyValueRow(next.get(0));
            if (dataLib.valuesEqual(resultKey, rowKey)) {
                List<KeyValue> result = next;
                next = null;
                return result;
            } else {
                final int comparison = hasher.toHashable(resultKey).compareTo(hasher.toHashable(rowKey));
                if (comparison > 0) {
                    // we are past the target key
                    // save the 'next' value for the next read
                    return Collections.emptyList();
                } else if (comparison < 0) {
                    // we need to seek to the target key
                    reader.seekOnRegionScanner(scanner, rowKey);
                    next = reader.nextResultsOnRegionScanner(scanner);
                    continue;
                } else {
                    throw new RuntimeException("Keys should not match");
                }
            }
        }
    }

}
