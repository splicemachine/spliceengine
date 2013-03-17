package com.splicemachine.perf.runner;

import com.splicemachine.derby.stats.Accumulator;
import com.splicemachine.derby.stats.Stats;
import com.splicemachine.derby.stats.TimeUtils;
import com.splicemachine.derby.stats.TimingStats;
import com.splicemachine.perf.runner.qualifiers.Qualifier;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 *         Created on: 3/15/13
 */
public class Query {

    private final String query;
    private final List<Qualifier> qualifiers;
    private final int samples;
    private final int threads;

    public Query(String query, List<Qualifier> qualifiers, int samples, int threads) {
        this.query = query;
        this.qualifiers = qualifiers;
        this.samples = samples;
        this.threads = threads;
    }

    public void run(final Connection conn) throws Exception{
        ExecutorService testRunner = Executors.newFixedThreadPool(threads);
        final int samplesPerThread = samples/threads;
        try{
            final QueryAccumulator accumulator = new QueryAccumulator();
            accumulator.start();
            CompletionService<Void> completionService = new ExecutorCompletionService<Void>(testRunner);
            for(int i=0;i<threads;i++){
                completionService.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        PreparedStatement ps = conn.prepareStatement(query);
                        for(int j=0;j<samplesPerThread;j++){
                            fillParameters(ps);
                            long start = System.nanoTime();
                            ResultSet resultSet = ps.executeQuery();
                            long numRecords = validate(resultSet);

                            accumulator.tick(numRecords,System.nanoTime()-start);
                        }
                        return null;
                    }
                });
            }

            //wait for all the threads to finish
            for(int i=0;i<threads;i++){
                Future<Void> future = completionService.take();
                future.get(); //check for errors
            }

            accumulator.finish();
            reportQueryStats(accumulator);
        }finally{
            testRunner.shutdown();
        }
    }

    private void reportQueryStats(QueryAccumulator accumulator) {
        //print query stats  to stdout
        System.out.printf("--------------------QUERY STATISTICS--------------------%n");
        System.out.printf("SQL query: %s%n",query);
        System.out.printf("%n");
        System.out.printf("\tNum threads: %d%n",threads);
        System.out.printf("\tNum samples: %d%n",samples);
        System.out.printf("\t%-25s\t%15d queries%n","Total queries executed",
                accumulator.recordStats.getTotalRecords());
        System.out.printf("\t%-25s\t%15d records%n","Total records retrieved",
                accumulator.timeStats.getTotalRecords());
        System.out.printf("\t%-25s\t%20.4f ms%n","Total time spent",
                TimeUtils.toMillis(accumulator.timeStats.getTotalTime()));

        System.out.printf("--------------------TIME DISTRIBUTION--------------------%n");
        Stats timeStats = accumulator.timeStats;
        System.out.printf("%-20s\t%20.4f ms%n","min",TimeUtils.toMillis(timeStats.getMinTime()));
        System.out.printf("%-20s\t%20.4f ms%n","median(p50)",TimeUtils.toMillis(timeStats.getMedian()));
        System.out.printf("%-20s\t%20.4f ms%n","p75",TimeUtils.toMillis(timeStats.get75P()));
        System.out.printf("%-20s\t%20.4f ms%n","p95",TimeUtils.toMillis(timeStats.get95P()));
        System.out.printf("%-20s\t%20.4f ms%n","p98",TimeUtils.toMillis(timeStats.get98P()));
        System.out.printf("%-20s\t%20.4f ms%n","p99",TimeUtils.toMillis(timeStats.get99P()));
        System.out.printf("%-20s\t%20.4f ms%n","p999",TimeUtils.toMillis(timeStats.get999P()));
        System.out.printf("%-20s\t%20.4f ms%n","max",TimeUtils.toMillis(timeStats.getMaxTime()));
        System.out.printf("%n");
        System.out.printf("%-20s\t%20.4f ms%n","avg",TimeUtils.toMillis(timeStats.getAvgTime()));
        System.out.printf("%-20s\t%20.4f ms%n","std. dev",TimeUtils.toMillis(timeStats.getTimeStandardDeviation()));
        System.out.println();
        System.out.printf("--------------------RECORD DISTRIBUTION--------------------%n");
        Stats recordStats = accumulator.recordStats;
        System.out.printf("%-20s\t%20d records%n","min",recordStats.getMinTime());
        System.out.printf("%-20s\t%20.4f records%n","median(p50)",recordStats.getMedian());
        System.out.printf("%-20s\t%20.4f records%n","p75",recordStats.get75P());
        System.out.printf("%-20s\t%20.4f records%n","p95",recordStats.get95P());
        System.out.printf("%-20s\t%20.4f records%n","p98",recordStats.get98P());
        System.out.printf("%-20s\t%20.4f records%n","p99",recordStats.get99P());
        System.out.printf("%-20s\t%20.4f records%n","p999",recordStats.get999P());
        System.out.printf("%-20s\t%20d records%n","max",recordStats.getMaxTime());
        System.out.printf("%n");
        System.out.printf("%-20s\t%20.4f records%n","avg",recordStats.getAvgTime());
        System.out.printf("%-20s\t%20.4f records%n","std. dev",recordStats.getTimeStandardDeviation());
        System.out.println();

    }

    private long validate(ResultSet resultSet) throws SQLException {
        int pos=1;
        long numRecords=0l;
        while(resultSet.next()){
            numRecords++;
            for(Qualifier qualifier:qualifiers){
                qualifier.validate(resultSet, pos);
            }
        }
        return numRecords;
    }

    public void fillParameters(PreparedStatement ps) throws Exception{
        int pos=1;
        for(Qualifier qualifier:qualifiers){
            qualifier.setInto(ps,pos);
            pos++;
        }
    }

    @Override
    public String toString() {
        return query;
    }

    private static class QueryAccumulator{
        private Accumulator timings = TimingStats.uniformSafeAccumulator();
        private Accumulator records = TimingStats.uniformSafeAccumulator();

        private Stats timeStats;
        private Stats recordStats;

        public void tick(long numRecords, long time){
            timings.tick(numRecords,time);
            records.tick(numRecords);
        }

        public void start() {
            timings.start();
        }

        public void finish() {
            timeStats = timings.finish();
            recordStats = records.finish();
        }
    }

}
