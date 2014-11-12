package com.splicemachine.si.txn;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.splicemachine.si.api.TimestampSource;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.ConsoleReporter;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Ignore;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 * Created on: 11/6/13
 */
@Ignore("Not a test")
public class ZooKeeperTimestampSourcePerformanceTest {
    private static final int numThreads = 10;
    private static final int numZkConnections = 1;
    private static final int numZkRetries = 3;
    private static final int zkRetryPauseMs = 1000;
    private static final int zkSessionTimeoutMs = 10000;
    private static final int numTimestampsPerThread = 10000;

    public static void main(String...args) throws Exception{
//        testMultipleThreadsSingleProcess();
        testMultipleProcess();
    }

    private static void testMultipleProcess() throws InterruptedException, IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        TimerSupplier statSupplier = new TimerSupplier(ZooKeeperStatTimestampSource.class);
        TimerSupplier oldSupplier = new TimerSupplier(ZooKeeperTimestampSource.class);
        final ZooKeeperSupplier zkSupplier = new ZooKeeperSupplier(numZkConnections);
        try{
            Supplier<TimestampSource> sourceSupplier = new Supplier<TimestampSource>() {
                @Override
                public TimestampSource get() {
                    return new ZooKeeperTimestampSource("/transactions",zkSupplier.get());
                }
            };
            testGeneration(sourceSupplier,ZooKeeperTimestampSource.class,oldSupplier,executorService,false);
            zkSupplier.reset();
            sourceSupplier = new Supplier<TimestampSource>() {
                @Override
                public TimestampSource get() {
                    return new ZooKeeperStatTimestampSource(zkSupplier.get(),"/txn");
                }
            };
            testGeneration(sourceSupplier,ZooKeeperStatTimestampSource.class, statSupplier,executorService,false);
        }finally{
            zkSupplier.close();
            printStats(statSupplier, oldSupplier);
            executorService.shutdownNow();
        }
    }

    private static void printStats(TimerSupplier statSupplier, TimerSupplier oldSupplier) {
        System.out.printf("Old TimestampSource Metrics: %n");
        oldSupplier.printResults();
        System.out.printf("Stat TimestampSource Metrics: %n");
        statSupplier.printResults();
        new ConsoleReporter(System.out).run();
    }

    private static RecoverableZooKeeper getZooKeeper()  {
        try {
            return new RecoverableZooKeeper("localhost:2181", ZooKeeperTimestampSourcePerformanceTest.zkSessionTimeoutMs,new Watcher() {
                                @Override
                                public void process(WatchedEvent watchedEvent) {
                                    System.out.println(watchedEvent);
                                }
                            }, ZooKeeperTimestampSourcePerformanceTest.numZkRetries, ZooKeeperTimestampSourcePerformanceTest.zkRetryPauseMs);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void testMultipleThreadsSingleProcess() throws InterruptedException, IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        TimerSupplier statSupplier = new TimerSupplier(ZooKeeperStatTimestampSource.class);
        TimerSupplier oldSupplier = new TimerSupplier(ZooKeeperTimestampSource.class);
        try{
            RecoverableZooKeeper rzk = getZooKeeper();
            TimestampSource timestampSource = new ZooKeeperTimestampSource("/transactions", rzk);
            Supplier<TimestampSource> sourceSupplier = Suppliers.ofInstance(timestampSource);
            testGeneration(sourceSupplier,timestampSource.getClass(),statSupplier, executorService,false);
            timestampSource = new ZooKeeperStatTimestampSource(rzk, "/txn");
            sourceSupplier = Suppliers.ofInstance(timestampSource);
            testGeneration(sourceSupplier,timestampSource.getClass(),oldSupplier, executorService,false);
        }finally{
            printStats(statSupplier, oldSupplier);
            executorService.shutdownNow();
        }
    }

    private static void testGeneration(Supplier<TimestampSource> sourceSupplier,
                                       Class<? extends TimestampSource> sourceClass,
                                       Supplier<Timer> timerSupplier,
                                       ExecutorService executorService,
                                       boolean globalTimer) throws InterruptedException, IOException {
        CompletionService<Void> executor = new ExecutorCompletionService<Void>(executorService);
        long start = System.currentTimeMillis();
        try{
            for(int i=0;i<numThreads;i++){
                executor.submit(new TestTask(sourceSupplier.get(),numTimestampsPerThread,timerSupplier.get(),!globalTimer));
            }

            for(int i=0;i<numThreads;i++){
                executor.take();
            }
        }finally{
            if(globalTimer)
                timerSupplier.get().stop();
            long stop = System.currentTimeMillis();
            System.out.printf("[%s]:Total time taken: %f s%n%n",sourceClass,(stop-start)/(1000d));
        }
    }

    private static class TestTask implements Callable<Void> {
        private final Timer rateTimer;
        private final Acquisition acquisition;

        private final int numTimestampsToFetch;
        private final boolean stopTimerOnFinish;

        public TestTask(TimestampSource timestampSource,
                        int numTimestampsToFetch,
                        Timer rateTimer,
                        boolean stopTimer) {
            this.acquisition = new Acquisition(timestampSource);
            this.rateTimer = rateTimer;
            this.numTimestampsToFetch = numTimestampsToFetch;
            this.stopTimerOnFinish = stopTimer;
        }

        @Override
        public Void call() throws Exception {
            try{
                for(int i=0;i<numTimestampsToFetch;i++){
                    rateTimer.time(acquisition);
                }
            }finally{
                if(stopTimerOnFinish)
                    rateTimer.stop();
            }
            return null;
        }
    }

    private static class Acquisition implements Callable<Void>{
        private final TimestampSource source;

        private Acquisition(TimestampSource source) {
            this.source = source;
        }

        @Override
        public Void call() throws Exception {
            source.nextTimestamp();
            return null;
        }
    }

    private static class TimerSupplier implements Supplier<Timer>{
        private List<Timer> timers = Lists.newArrayList();
        private final AtomicInteger counter = new AtomicInteger(0);
        private final Class<? extends TimestampSource> clazz;

        private TimerSupplier(Class<? extends TimestampSource> clazz) {
            this.clazz = clazz;
        }

        @Override
        public Timer get() {
            MetricName name = new MetricName(clazz,"timer-"+counter.incrementAndGet());
            Timer timer = Metrics.newTimer(name,TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
            timers.add(timer);
            return timer;
        }

        public void printResults(){
            long count = 0l;
            double meanRate = 0;
            double meanOneMinuteRate = 0;
            double meanFiveMinuteRate = 0;
            double meanFifteenMinuteRate = 0;

            for(Timer timer:timers){
                double rate = timer.meanRate();
                double oneMinuteRate = timer.oneMinuteRate();
                double fiveMinuteRate = timer.fiveMinuteRate();
                double fifteenMinuteRate = timer.fifteenMinuteRate();

                meanRate +=rate;
                meanOneMinuteRate +=oneMinuteRate;
                meanFiveMinuteRate +=fiveMinuteRate;
                meanFifteenMinuteRate +=fifteenMinuteRate;
                count+= timer.count();
            }

            String eventType = timers.get(0).eventType();
            System.out.printf("              count = %d\n",count);
            System.out.printf("              mean rate = %2.2f %s\n",meanRate, eventType);
            System.out.printf("              1-minute rate = %2.2f %s\n",meanOneMinuteRate,eventType);
            System.out.printf("              5-minute rate = %2.2f %s\n",meanFiveMinuteRate,eventType);
            System.out.printf("              15-minute rate = %2.2f %s\n",meanFifteenMinuteRate,eventType);
            System.out.printf("======================================%n%n");
        }
    }

    private static class ZooKeeperSupplier implements Supplier<RecoverableZooKeeper>{
        private final List<RecoverableZooKeeper> instances = Lists.newArrayList();
        private final List<RecoverableZooKeeper> outstandingInstances = Lists.newArrayList();
        private final int maxInstances;
        private int position = -1;

        private ZooKeeperSupplier(int maxInstances) {
            this.maxInstances = maxInstances;
        }

        @Override
        public RecoverableZooKeeper get() {
            RecoverableZooKeeper instance;

            if(instances.size()>0){
                instance = instances.remove(0);
            }else if(outstandingInstances.size()<maxInstances){
                instance = getZooKeeper();
            }else{
                //re-use an existing connection
                position = (position+1) % outstandingInstances.size();
                instance = outstandingInstances.get(position);
            }
            outstandingInstances.add(instance);
            return instance;
        }

        public void reset(){
            instances.addAll(outstandingInstances);
            outstandingInstances.clear();
        }

        public void close() throws InterruptedException {
            for(RecoverableZooKeeper rzk:instances){
                rzk.close();
            }
            for(RecoverableZooKeeper rzk:outstandingInstances){
                rzk.close();
            }
        }
    }

}
