/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.stream.iterator;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.utils.StreamLogUtils;
import com.splicemachine.derby.utils.Scans;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 *
 */
@NotThreadSafe
public class TableScannerIterator implements Iterable<LocatedRow>, Iterator<LocatedRow>, Closeable {
    protected TableScannerBuilder siTableBuilder;
    protected SITableScanner tableScanner;
    protected boolean initialized;
    private ExecRow execRow;
    boolean slotted;
    boolean hasNext;
    int rows = 0;
    protected ScanOperation operation;
    protected Qualifier[][] qualifiers;
    protected int[] baseColumnMap;
    protected boolean rowIdKey; // HACK Row ID Qualifiers point to the projection above them ?  TODO JL

    public TableScannerIterator(TableScannerBuilder siTableBuilder, SpliceOperation operation) throws StandardException {
        this.siTableBuilder = siTableBuilder;
        this.operation = (ScanOperation) operation;
        if (operation != null) {
            this.qualifiers = ((ScanOperation) operation).getScanInformation().getScanQualifiers();
            this.baseColumnMap = ((ScanOperation) operation).getOperationInformation().getBaseColumnMap();
            this.rowIdKey = ((ScanOperation) operation).getRowIdKey();
        }
    }

    @Override
    public Iterator<LocatedRow> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        try {
            if (slotted)
                return hasNext;
            slotted = true;
            if (!initialized) {
                initialized = true;
                tableScanner = siTableBuilder.build();
                tableScanner.open();
                if (operation!= null) {
                    operation.registerCloseable(new Closeable() {
                        @Override
                        public void close() throws IOException {
                            try {
                                if (tableScanner != null && initialized)
                                    tableScanner.close();
                            } catch (Exception e) {
                                throw new IOException(e);
                            }
                        }
                    });
                }
            }
            while (true) {
                execRow = tableScanner.next();
                if (execRow == null) {
                    tableScanner.close();
                    initialized = false;
                    hasNext = false;
                    return hasNext;
                } else {
                    hasNext = true;
                    if (qualifiers == null || rowIdKey || Scans.qualifyRecordFromRow(execRow.getRowArray(), qualifiers,baseColumnMap,siTableBuilder.getOptionalProbeValue() ))
                        break;
                }
            }
            return hasNext;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public LocatedRow next() {
        slotted = false;
        rows++;
        LocatedRow locatedRow = new LocatedRow(tableScanner.getCurrentRowLocation(),execRow.getClone());
        if (operation != null) {
            StreamLogUtils.logOperationRecord(locatedRow, operation);
            operation.setCurrentLocatedRow(locatedRow);
        }
        return locatedRow;
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public void close() throws IOException {
        if (tableScanner != null) {
            try {
                tableScanner.close();
            } catch (StandardException se) {
                throw new IOException(se);
            }
        }
    }
}
