package com.splicemachine.perf.runner;

import com.splicemachine.perf.runner.qualifiers.Result;
import com.splicemachine.tools.ConnectionPool;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
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
    private final int poolSize;
    private ConnectionPool connectionPool;

    public Data(String server,
                int concurrentQueries, List<Table> tables,
                List<Query> queries, List<Index> indices, int poolSize) {
        this.concurrentQueries = concurrentQueries;
        this.tables = tables;
        this.server = server;
        this.queries = queries;
        this.indices = indices;
        this.poolSize = poolSize;
    }

    public void connect() throws Exception {
        connectionPool = ConnectionPool.create(new ConnectionPool.Supplier() {
            @Override
            public Connection createNew() throws SQLException {
                return DriverManager.getConnection("jdbc:derby://"+server+"/wombat;create=true");
            }
        }, poolSize);
    }

    public void createTables() throws Exception {
        //create Tables
        SpliceLogUtils.info(LOG,"Creating tables");
        Connection connection = connectionPool.acquire();
        try{
            for(Table table:tables){
                table.create(connection);
            }
            connection.commit();
        }finally{
            connection.close();
        }
    }

    public void createIndices() throws Exception {
        SpliceLogUtils.info(LOG,"Creating indices");
        Connection conn = connectionPool.acquire();
        try{
            for(Index index:indices){
                index.create(conn);
            }
            conn.commit();
        }finally{
            conn.close();
        }
    }

    public void loadData() throws Exception  {
        //load data into each table
        SpliceLogUtils.info(LOG,"Loading data");
        for(Table table:tables){
            table.insertData(connectionPool).write(System.out);
        }
        SpliceLogUtils.info(LOG, "Data loading, ready to perform queries");
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
                        return query.run(connectionPool);
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

    public void dropTables() throws Exception {
        SpliceLogUtils.info(LOG, "dropping tables");
        PreparedStatement dropStatement;
        Connection conn = connectionPool.acquire();
        try{
        for(Table table:tables){
            dropStatement = conn.prepareStatement("drop table "+ table.getName());
            dropStatement.execute();
        }
        conn.commit();
        }finally{
            conn.close();
        }
    }

    public void shutdown() throws Exception {
        connectionPool.shutdown();
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
