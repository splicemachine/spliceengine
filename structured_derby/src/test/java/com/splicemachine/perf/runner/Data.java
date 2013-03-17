package com.splicemachine.perf.runner;

import com.google.common.collect.Maps;
import com.splicemachine.derby.stats.Accumulator;
import com.splicemachine.derby.stats.Stats;
import com.splicemachine.derby.stats.TimingStats;
import com.splicemachine.derby.stats.TimeUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 *         Created on: 3/15/13
 */
public class Data {
    private static final Logger LOG = Logger.getLogger(Data.class);
    private final String server;
    private final int insertThreads;
    private final List<Table> tables;
    private final List<Query> queries;
    private Connection conn;

    public Data(String server, int insertThreads, List<Table> tables, List<Query> queries) {
        this.insertThreads = insertThreads;
        this.tables = tables;
        this.server = server;
        this.queries = queries;
    }

    public void connect() throws SQLException{
        conn = DriverManager.getConnection("jdbc:derby://"+server+"/wombat;create=true");
    }

    public void createTables() throws SQLException {
        //create Tables
        SpliceLogUtils.info(LOG,"Creating tables");
        for(Table table:tables){
            SpliceLogUtils.trace(LOG, "Creating table %s with statement %s", table.getName(),table.getCreateTableString());
            PreparedStatement ps = table.getCreateTableStatement(conn);
            ps.execute();
        }
    }

    public void loadData() throws Exception  {

        //load data into each table
        SpliceLogUtils.info(LOG,"Loading data");
        ExecutorService dataLoader = Executors.newFixedThreadPool(insertThreads);

        final Map<Table,Accumulator> runningInsertsMap = Maps.newConcurrentMap();
        CompletionService<Void> completionService = new ExecutorCompletionService<Void>(dataLoader);
        try{
            int numTasks=0;
            for(final Table table:tables){
                final int numToInsert = table.getNumRows()/insertThreads;
                final Accumulator accumulator = TimingStats.uniformSafeAccumulator();
                accumulator.start();
                runningInsertsMap.put(table, accumulator);
                for(int i=0;i<insertThreads;i++){
                    numTasks++;
                    completionService.submit(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            PreparedStatement ps = table.getInsertDataStatement(conn);
                            for(int count=0;count<numToInsert;count++){
                                table.fillStatement(ps);
                                long start = System.nanoTime();
                                ps.execute();
                                accumulator.tick(System.nanoTime()-start);
                            }
                            return null;
                        }
                    });
                }
            }

            while(numTasks>0){
                Future<Void> take = completionService.take();
                //check for errors, and bail if one is encountered
                take.get();
                numTasks--;
            }
            printInsertStatistics(runningInsertsMap);
            SpliceLogUtils.info(LOG,"Data loading, ready to perform queries");
        }finally{
            dataLoader.shutdown();
        }
    }

    public void runQueries() throws Exception {
        for(Query query:queries){
            SpliceLogUtils.trace(LOG,"Executing query '%s'",query);
            query.run(conn);
        }
    }

    public void dropTables() throws SQLException {
        SpliceLogUtils.info(LOG, "dropping tables");
        PreparedStatement dropStatement;
        for(Table table:tables){
            dropStatement = conn.prepareStatement("drop table "+ table.getName());
            dropStatement.execute();
        }
    }

    public void shutdown() throws SQLException{
        if(conn!=null)
            conn.close();
    }

    public static void printInsertStatistics( Map<Table,Accumulator> statsMap){
        for(Table table:statsMap.keySet()){
            Stats stats = statsMap.get(table).finish();

            //log the table stats to stdout
            System.out.printf("Insertion stats:%n");
            System.out.printf("\t%-25s\t%15d records%n","Total records inserted", stats.getTotalRecords());
            System.out.printf("\t%-25s\t%20.4f ms%n","Total time spent", TimeUtils.toMillis(stats.getTotalTime()));
            System.out.printf("---------------------TIME DISTRIBUTION----------------------------%n");
            System.out.printf("%-20s\t%20.4f ms%n","min",TimeUtils.toMillis(stats.getMinTime()));
            System.out.printf("%-20s\t%20.4f ms%n","median(p50)",TimeUtils.toMillis(stats.getMedian()));
            System.out.printf("%-20s\t%20.4f ms%n","p75",TimeUtils.toMillis(stats.get75P()));
            System.out.printf("%-20s\t%20.4f ms%n","p95",TimeUtils.toMillis(stats.get95P()));
            System.out.printf("%-20s\t%20.4f ms%n","p98",TimeUtils.toMillis(stats.get98P()));
            System.out.printf("%-20s\t%20.4f ms%n","p99",TimeUtils.toMillis(stats.get99P()));
            System.out.printf("%-20s\t%20.4f ms%n","p999",TimeUtils.toMillis(stats.get999P()));
            System.out.printf("%-20s\t%20.4f ms%n","max",TimeUtils.toMillis(stats.getMaxTime()));
            System.out.printf("%n");
            System.out.printf("%-20s\t%20.4f ms%n","avg",TimeUtils.toMillis(stats.getAvgTime()));
            System.out.printf("%-20s\t%20.4f ms%n","std. dev",TimeUtils.toMillis(stats.getTimeStandardDeviation()));
            System.out.println();
        }
    }

    @Override
    public String toString() {
        return "Data{" +
                "server='" + server + '\'' +
                ", tables=" + tables +
                ", queries=" + queries +
                '}';
    }

}
