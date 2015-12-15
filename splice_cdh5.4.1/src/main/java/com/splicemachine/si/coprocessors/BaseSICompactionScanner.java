package com.splicemachine.si.coprocessors;

import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.server.SICompactionState;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Decorator for an HBase scanner that performs SI operations at compaction time. Delegates the core work to
 * SICompactionState.
 */
public abstract class BaseSICompactionScanner implements InternalScanner {
    private final SICompactionState compactionState;
    private final InternalScanner delegate;
    private final SDataLib dataLib = SIDriver.getDataLib();
    private List<Cell> rawList =new ArrayList<>();

    public BaseSICompactionScanner(SICompactionState compactionState,
                               InternalScanner scanner) {
        this.compactionState = compactionState;
        this.delegate = scanner;
    }

    /**
     * Read data from the underlying scanner and send the results through the SICompactionState.
     */
    @SuppressWarnings("unchecked")
	protected boolean nextDirect(List<Cell> results, int limit) throws IOException {
        rawList.clear();
        final boolean more = ((HDataLib)dataLib).internalScannerNext(delegate, rawList);
		compactionState.mutate(rawList, results);
		return more;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}