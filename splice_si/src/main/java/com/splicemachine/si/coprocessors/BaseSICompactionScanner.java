package com.splicemachine.si.coprocessors;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SICompactionState;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Decorator for an HBase scanner that performs SI operations at compaction time. Delegates the core work to
 * SICompactionState.
 */
public abstract class BaseSICompactionScanner<Data,
Put extends OperationWithAttributes,Delete,Get extends OperationWithAttributes, Scan> implements InternalScanner {
    private final  SICompactionState compactionState;
    private final InternalScanner delegate;
    private final SDataLib<Data,Put,Delete,Get,Scan> dataLib;
    List<Data> rawList = new ArrayList<>();

    public BaseSICompactionScanner(SICompactionState compactionState,
                               InternalScanner scanner, SDataLib<Data,Put,Delete,Get,Scan> dataLib) {
        this.compactionState = compactionState;
        this.delegate = scanner;
        this.dataLib = dataLib;
    }

    /**
     * Read data from the underlying scanner and send the results through the SICompactionState.
     */
    @SuppressWarnings("unchecked")
	protected boolean nextDirect(List<Data> results, int limit) throws IOException {
        rawList.clear();
        final boolean more = dataLib.internalScannerNext(delegate, rawList);
		compactionState.mutate(rawList, results);
		return more;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}