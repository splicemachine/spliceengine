package com.splicemachine.perf.runner;

import com.splicemachine.derby.stats.Accumulator;
import com.splicemachine.derby.stats.Stats;
import com.splicemachine.derby.stats.TimeUtils;
import com.splicemachine.derby.stats.TimingStats;
import com.splicemachine.perf.runner.qualifiers.Qualifier;
import com.splicemachine.perf.runner.qualifiers.Result;
import com.splicemachine.tools.ConnectionPool;
import org.apache.log4j.Logger;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Created on: 3/15/13
 */
public class Query {
    private final String query;
    private final List<Qualifier> qualifiers;
    private final int samples;
    private final int threads;
    private final Map<String,Integer> printColumns;
    private static final Logger LOG = Logger.getLogger(Query.class);

    public Query(String query, List<Qualifier> qualifiers, int samples, int threads, Map<String, Integer> printColumns) {
        this.query = query;
        this.qualifiers = qualifiers;
        this.samples = samples;
        this.threads = threads;
        this.printColumns = printColumns;
    }

    public Result run(final ConnectionPool connectionPool) throws Exception{
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
                        Connection conn = connectionPool.acquire();
                        try{
                            PreparedStatement ps = conn.prepareStatement(query);
                            for(int j=0;j<samplesPerThread;j++){
                                fillParameters(ps);
                                long start = System.nanoTime();
                                ResultSet resultSet = ps.executeQuery();
                                long numRecords = processResults(resultSet);

                                accumulator.tick(numRecords,System.nanoTime()-start);
                            }
                            return null;
                        }finally{
                            conn.close();
                        }
                    }
                });
            }

            //wait for all the threads to finish
            for(int i=0;i<threads;i++){
                Future<Void> future = completionService.take();
                future.get(); //check for errors
            }

            accumulator.finish();
            return new QueryResult(accumulator);
        }finally{
            testRunner.shutdown();
        }
    }

    private class QueryResult implements Result{
        private final QueryAccumulator accumulator;
        private QueryResult(QueryAccumulator accumulator) {
            this.accumulator = accumulator;
        }

        @Override
        public void write(PrintStream stream) throws Exception {
            stream.printf("--------------------QUERY STATISTICS--------------------%n");
            stream.printf("SQL query: %s%n",Query.this.query);
            stream.printf("%n");
            stream.printf("\tNum threads: %d%n",Query.this.threads);
            stream.printf("\tNum samples: %d%n",Query.this.samples);
            stream.printf("\t%-25s\t%15d queries%n","Total queries executed",
                    accumulator.recordStats.getTotalRecords());
            stream.printf("\t%-25s\t%15d records%n","Total records retrieved",
                    accumulator.timeStats.getTotalRecords());
            stream.printf("\t%-25s\t%20.4f ms%n","Total time spent",
                    TimeUtils.toMillis(accumulator.timeStats.getTotalTime()));

            stream.printf("--------------------TIME DISTRIBUTION--------------------%n");
            Stats timeStats = accumulator.timeStats;
            stream.printf("%-20s\t%20.4f ms%n","min",TimeUtils.toMillis(timeStats.getMinTime()));
            stream.printf("%-20s\t%20.4f ms%n","median(p50)",TimeUtils.toMillis(timeStats.getMedian()));
            stream.printf("%-20s\t%20.4f ms%n","p75",TimeUtils.toMillis(timeStats.get75P()));
            stream.printf("%-20s\t%20.4f ms%n","p95",TimeUtils.toMillis(timeStats.get95P()));
            stream.printf("%-20s\t%20.4f ms%n","p98",TimeUtils.toMillis(timeStats.get98P()));
            stream.printf("%-20s\t%20.4f ms%n","p99",TimeUtils.toMillis(timeStats.get99P()));
            stream.printf("%-20s\t%20.4f ms%n","p999",TimeUtils.toMillis(timeStats.get999P()));
            stream.printf("%-20s\t%20.4f ms%n","max",TimeUtils.toMillis(timeStats.getMaxTime()));
            stream.printf("%n");
            stream.printf("%-20s\t%20.4f ms%n","avg",TimeUtils.toMillis(timeStats.getAvgTime()));
            stream.printf("%-20s\t%20.4f ms%n","std. dev",TimeUtils.toMillis(timeStats.getTimeStandardDeviation()));
            stream.println();
            stream.printf("--------------------RECORD DISTRIBUTION--------------------%n");
            Stats recordStats = accumulator.recordStats;
            stream.printf("%-20s\t%20d records%n","min",recordStats.getMinTime());
            stream.printf("%-20s\t%20.4f records%n","median(p50)",recordStats.getMedian());
            stream.printf("%-20s\t%20.4f records%n","p75",recordStats.get75P());
            stream.printf("%-20s\t%20.4f records%n","p95",recordStats.get95P());
            stream.printf("%-20s\t%20.4f records%n","p98",recordStats.get98P());
            stream.printf("%-20s\t%20.4f records%n","p99",recordStats.get99P());
            stream.printf("%-20s\t%20.4f records%n","p999",recordStats.get999P());
            stream.printf("%-20s\t%20d records%n","max",recordStats.getMaxTime());
            stream.printf("%n");
            stream.printf("%-20s\t%20.4f records%n","avg",recordStats.getAvgTime());
            stream.printf("%-20s\t%20.4f records%n","std. dev",recordStats.getTimeStandardDeviation());
            stream.println();
        }
    }

    private long processResults(ResultSet rs) throws Exception {
        long resultCount=0;
        StringBuilder results = null;
        if(printColumns!=null)
            results = new StringBuilder("results for query ").append(query).append("\n");

        while(rs.next()){
            resultCount++;
            for(Qualifier qualifier: qualifiers){
                qualifier.validate(rs);
            }

            if(results!=null){
                results = results.append("row:").append(resultCount).append(",");
                results = results.append(stringifyRow(rs)).append("\n");
            }

        }
        if(results!=null){
            LOG.info(results.toString());
        }

        return resultCount;
    }

    private String stringifyRow(ResultSet rs) throws Exception {
        boolean isStart=true;
        StringBuilder results = new StringBuilder();
        for(String colName:printColumns.keySet()){
            if(!isStart)results = results.append(",");
            else isStart=false;

            int colNum = printColumns.get(colName);
            results = results.append(colName).append(":").append(rs.getObject(colNum));
        }
        return results.toString();
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
