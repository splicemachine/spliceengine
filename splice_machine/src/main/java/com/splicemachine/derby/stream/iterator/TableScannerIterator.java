package com.splicemachine.derby.stream.iterator;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Iterator;

/**
 * Created by jleach on 4/29/15.
 */
@NotThreadSafe
public class TableScannerIterator implements Iterable<LocatedRow>, Iterator<LocatedRow> {
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
                tableScanner = siTableBuilder.build();
                tableScanner.open();
            }
            execRow = tableScanner.next(null);
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
        operation.setCurrentRow(locatedRow.getRow());
        operation.setCurrentRowLocation(locatedRow.getRowLocation());
        return locatedRow;
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not Implemented");
    }
}
