package com.splicemachine.async;

import com.splicemachine.metrics.MetricFactory;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 * Executes multiple scans in parallel
 * @author Scott Fines
 *         Date: 12/15/14
 */
public class SortedMultiScanner extends BaseAsyncScanner{
    private final AsyncScanner[] subScanner;
    private final Comparator<byte[]> sortComparator;
    private final List<KeyValue>[] nextAnswers;
    private final boolean[] exhaustedScanners;

    @SuppressWarnings("unchecked")
    public SortedMultiScanner(List<Scanner> scanners,
                              int maxQueueSize,
                              Comparator<byte[]> sortComparator,
                              MetricFactory metricFactory){
        super(metricFactory);
        this.sortComparator = sortComparator;

        this.subScanner = new AsyncScanner[scanners.size()];
        for(int i=0;i<scanners.size();i++){
            Scanner scanner = scanners.get(i);
            this.subScanner[i] = new QueueingAsyncScanner(scanner,maxQueueSize,metricFactory);
        }
        this.nextAnswers = new List[scanners.size()];
        this.exhaustedScanners = new boolean[scanners.size()];
    }

    @Override
    public List<KeyValue> nextKeyValues() throws Exception {
        timer.startTiming();
        List<KeyValue> currMinList = null;
        KeyValue currMinFirst = null;
        int currMinPos = -1;
        for(int i=0;i<nextAnswers.length;i++){
            if(exhaustedScanners[i]) continue;

            List<KeyValue> next;
            if(nextAnswers[i]!=null)
                next = nextAnswers[i];
            else{
                /*
                 * We used this value last time, make sure it's filled
                 */
                next = subScanner[i].nextKeyValues();
                if(next==null || next.size()<=0){
                    exhaustedScanners[i] = true;
                    subScanner[i].close();
                    continue;
                }
                nextAnswers[i] = next;
            }

            if(currMinFirst==null){
                currMinFirst = next.get(0);
                currMinList = next;
                currMinPos = i;
            }else{
                KeyValue first = next.get(0);
                if(sortComparator.compare(first.key(),currMinFirst.key())<0){
                    currMinList = next;
                    currMinFirst = first;
                    currMinPos = i;
                }
            }
        }
        if(currMinFirst==null)
            timer.stopTiming();
        else{
            timer.tick(1);
            nextAnswers[currMinPos] = null;
        }
        return currMinList;
    }

    @Override
    public void open() throws IOException {
        for(AsyncScanner scanner:subScanner){
            scanner.open();
        }
    }

    @Override
    public void close() {
        for(int i=0;i<subScanner.length;i++){
            if(!exhaustedScanners[i])
                subScanner[i].close();
        }
    }
}
