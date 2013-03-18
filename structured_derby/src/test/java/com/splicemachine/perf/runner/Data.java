package com.splicemachine.perf.runner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.stats.Accumulator;
import com.splicemachine.derby.stats.Stats;
import com.splicemachine.derby.stats.TimingStats;
import com.splicemachine.derby.stats.TimeUtils;
import com.splicemachine.perf.runner.qualifiers.Result;
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
    private final int concurrentQueries;
    private final List<Table> tables;
    private final List<Query> queries;
    private final List<Index> indices;
    private Connection conn;

    public Data(String server,
                int concurrentQueries, List<Table> tables,
                List<Query> queries, List<Index> indices) {
        this.concurrentQueries = concurrentQueries;
        this.tables = tables;
        this.server = server;
        this.queries = queries;
        this.indices = indices;
    }

    public void connect() throws SQLException{
        conn = DriverManager.getConnection("jdbc:derby://"+server+"/wombat;create=true");
        conn.setAutoCommit(false);
    }

    public void createTables() throws SQLException {
        //create Tables
        SpliceLogUtils.info(LOG,"Creating tables");
        for(Table table:tables){
            table.create(conn);
        }
        conn.commit();
    }

    public void createIndices() throws Exception {
        SpliceLogUtils.info(LOG,"Creating indices");
        for(Index index:indices){
            index.create(conn);
        }
        conn.commit();
    }

    public void loadData() throws Exception  {
        //load data into each table
        SpliceLogUtils.info(LOG,"Loading data");
        for(Table table:tables){
            table.insertData(conn).write(System.out);
        }
        SpliceLogUtils.info(LOG,"Data loading, ready to perform queries");
        conn.commit();
    }

    public void runQueries() throws Exception {
        ExecutorService queryRunner = Executors.newFixedThreadPool(concurrentQueries);
        try{
            CompletionService<Result> runningQueries = new ExecutorCompletionService<Result>(queryRunner);
            for(final Query query:queries){
                runningQueries.submit(new Callable<Result>() {
                    @Override
                    public Result call() throws Exception {
                        SpliceLogUtils.trace(LOG,"Executing query '%s'",query);
                        return query.run(conn);
                    }
                });
            }
            int numTasks = queries.size();
            while(numTasks>0){
                Future<Result> result = runningQueries.take();
                Result r = result.get();
                r.write(System.out);
                numTasks--;
            }
        }finally{
            queryRunner.shutdown();
        }
    }

    public void dropTables() throws SQLException {
        SpliceLogUtils.info(LOG, "dropping tables");
        PreparedStatement dropStatement;
        for(Table table:tables){
            dropStatement = conn.prepareStatement("drop table "+ table.getName());
            dropStatement.execute();
        }
        conn.commit();
    }

    public void shutdown() throws SQLException{
        if(conn!=null)
            conn.close();
    }

    public static void printInsertStatistics( Map<Table,Accumulator> statsMap){
        for(Table table:statsMap.keySet()){
            Stats stats = statsMap.get(table).finish();


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
