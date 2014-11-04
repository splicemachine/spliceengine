package com.splicemachine.perf.runner;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.perf.runner.qualifiers.Result;
import com.splicemachine.tools.ConnectionPool;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.List;
import java.util.Properties;
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
        connectionPool = ConnectionPool.create(new Loader(), poolSize);
    }

    public void createTables() throws Exception {
        //create Tables
        SpliceLogUtils.info(LOG,"Creating tables");
        for(Table table:tables){
            table.create(connectionPool);
        }
    }

    public void createIndices() throws Exception {
        SpliceLogUtils.info(LOG,"Creating indices");
        if(indices!=null){
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
    }

    public void loadData() throws Exception  {
        //load data into each table
        SpliceLogUtils.info(LOG,"Loading data");
        for(Table table:tables){
            Result result = table.insertData(connectionPool);
            if(result!=null)
                result.write(System.out);
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

    public void dropTables(boolean explodeOnError) throws Exception {
        SpliceLogUtils.info(LOG, "dropping tables");
        PreparedStatement dropStatement;
        Connection conn = connectionPool.acquire();
        try{
            for(Table table:tables){
                try{
                    dropStatement = conn.prepareStatement("drop table "+ table.getName());
                    dropStatement.execute();
                }catch(SQLException se){
                    if(explodeOnError)
                        throw se;
                    else
                        SpliceLogUtils.warn(LOG,"Encountered error dropping tables:"+se.getMessage());
                }
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
    private class Loader implements ConnectionPool.Supplier{
        private final Driver driver;
        private final String jdbcPath;

        private Loader()  throws Exception{
            //attempt to load the driver
            Class.forName("org.apache.derby.jdbc.AutoloadedDriver40");
            //fetch the driver for the url
            jdbcPath = "jdbc:derby://"+server+"/" + SpliceConstants.SPLICE_DB + ";create=true";
            driver = DriverManager.getDriver(jdbcPath);
        }

        @Override
        public Connection createNew() throws SQLException {
            return driver.connect(jdbcPath,new Properties());
        }
    }
}
