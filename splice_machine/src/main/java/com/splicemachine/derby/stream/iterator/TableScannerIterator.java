package com.splicemachine.derby.stream.iterator;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.utils.StreamLogUtils;
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
    protected SpliceOperation operation;

    public TableScannerIterator(TableScannerBuilder siTableBuilder, SpliceOperation operation) {
        this.siTableBuilder = siTableBuilder;
        this.operation = operation;
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
            execRow = tableScanner.next();
            if (execRow==null) {
                tableScanner.close();
                initialized = false;
                hasNext = false;
                return hasNext;
            } else {
                hasNext = true;
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
