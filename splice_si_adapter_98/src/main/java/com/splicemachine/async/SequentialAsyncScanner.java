package com.splicemachine.async;

import com.splicemachine.metrics.MetricFactory;
import com.stumbleupon.async.Deferred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An AsyncScanner which does sequential execution.
 * This doesn't have ideal throughput, but is useful for testing out issues with other Async Scanners.
 *
 * @author Scott Fines
 *         Date: 12/15/14
 */
public class SequentialAsyncScanner extends BaseAsyncScanner{

    private final Scanner scanner;
    private ArrayList<ArrayList<KeyValue>> buffer;
    private boolean finished = false;

    public SequentialAsyncScanner(Scanner scanner,
                                  MetricFactory metricFactory) {
        super(metricFactory);
        this.scanner = scanner;
    }

    @Override
    public void open() throws IOException {
        //no-op
    }

    @Override
    public void close() {
        try {
            scanner.close().join();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<KeyValue> nextKeyValues() throws Exception {
        if(buffer!=null && buffer.size()>0)
            return buffer.remove(0);

        if(finished) return null; //scanner is exhausted

        timer.startTiming();
        buffer = scanner.nextRows().join();
        if(buffer==null || buffer.size()<=0){
            timer.stopTiming();
            finished=true;
            return null;
        }
        timer.tick(buffer.size());

        if(scanner.onFinalRegion() && buffer.size()<scanner.getMaxNumRows()){
            finished=true;
        }
        return buffer.remove(0);
    }

}
