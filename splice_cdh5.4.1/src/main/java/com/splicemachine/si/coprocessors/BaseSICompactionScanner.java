package com.splicemachine.si.coprocessors;

import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.server.SICompactionState;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Decorator for an HBase scanner that performs SI operations at compaction time. Delegates the core work to
 * SICompactionState.
 */
public abstract class BaseSICompactionScanner<Cell,
Put extends OperationWithAttributes,Delete,Get extends OperationWithAttributes, Scan> implements InternalScanner {
    protected final SICompactionState compactionState;
    protected final InternalScanner delegate;
    protected final SDataLib dataLib = SIDriver.getDataLib();
    protected List<Cell> rawList = new ArrayList<Cell>();

    public BaseSICompactionScanner(SICompactionState compactionState,
                               InternalScanner scanner) {
        this.compactionState = compactionState;
        this.delegate = scanner;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}