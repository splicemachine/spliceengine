package com.splicemachine.perf.runner;

import com.google.common.collect.Lists;
import com.splicemachine.derby.stats.Stats;
import com.splicemachine.metrics.DisplayTime;
import com.splicemachine.perf.runner.qualifiers.Result;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.Timer;
import com.splicemachine.tools.ConnectionPool;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 *Created on: 3/15/13
 */
public class Table {
    private static final Logger LOG = Logger.getLogger(Table.class);
    private final String name;
    private final List<Column> columns;
    private final int numRows;
    private final int insertBatch;
    private final int insertThreads;
    private final List<String> files;

    public Table(String name, List<Column> columns, int numRows, int insertBatch, int insertThreads, List<String> filesToImport) {
        this.name = name;
        this.columns = columns;
        this.numRows = numRows;
        this.insertBatch = insertBatch;
        this.insertThreads = insertThreads;
        this.files = filesToImport;
    }

    public void create(ConnectionPool pool) throws SQLException, InterruptedException {
        SpliceLogUtils.debug(LOG, "Creating table %s",name);
        Connection conn = null;
        try{
            conn = pool.acquire();
            String createStr = getCreateTableString();
            SpliceLogUtils.trace(LOG,"using sql "+ createStr);
            PreparedStatement ps = conn.prepareStatement(createStr);
            ps.execute();
        }finally{
            if(conn!=null)
                conn.close();
        }
    }

    public Result insertData(final ConnectionPool connectionPool) throws Exception{
        if(files !=null){
            return importData(connectionPool);
        }else{
            return generateAndInsertData(connectionPool);
        }
    }

    private Result importData(ConnectionPool connectionPool) throws InterruptedException, SQLException {
        ImportResult stats = new ImportResult();
        long start = System.nanoTime();
        for(String file: files){
            Connection connection = connectionPool.acquire();
            try{
                SpliceLogUtils.debug(LOG, "Loading data from file %s", file);
                long fileStart = System.nanoTime();
                PreparedStatement s = connection.prepareStatement("call SYSCS_UTIL.SYSCS_IMPORT_DATA(null,'"+name.toUpperCase()+"',null,null,'"+file+"',',',null,null)");
                s.execute();
                long stop = System.nanoTime();
                stats.addTime(file,stop-fileStart);
            }finally{
                connection.close();
            }
        }
        stats.finalize(System.nanoTime()-start);
        return stats;
    }

    private Result generateAndInsertData(final ConnectionPool connectionPool) throws InterruptedException, ExecutionException {
        ExecutorService dataInserter = Executors.newFixedThreadPool(insertThreads);
        try{
            List<Future<Void>> futures = Lists.newArrayListWithCapacity(insertThreads + 1);

            for(int i=0;i<insertThreads;i++){
                futures.add(dataInserter.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        final Timer insertTimer = Metrics.newTimer();
                        int rowsToInsert = numRows/insertThreads;
                        int tenPercentSize = rowsToInsert>10?rowsToInsert/10 : 1;
                        int written = 0;
                        Connection conn = connectionPool.acquire();
                        conn.setAutoCommit(false);
                        try{
                            PreparedStatement ps = conn.prepareStatement(getInsertDataString());
                            for(int numRowsInserted=0;numRowsInserted<numRows/insertThreads;numRowsInserted++){
                                fillStatement(ps);
                                ps.addBatch();
                                written++;
                                if(numRowsInserted>0&&numRowsInserted%insertBatch==0){
                                    insertTimer.startTiming();
                                    int numInserted = ps.executeBatch().length;
                                    insertTimer.tick(numInserted);

                                    if((written-1)%tenPercentSize==0){
                                        conn.commit();
                                        SpliceLogUtils.info(LOG, "%d%% complete", ((written - 1) / tenPercentSize) * 10);
                                    }
                                }
                            }
                            insertTimer.startTiming();
                            int numInserted = ps.executeBatch().length;
                            if(numInserted>0){
                                insertTimer.tick(numInserted);
                                SpliceLogUtils.info(LOG,"writing complete");
                            }else
                                insertTimer.stopTiming();
                            conn.commit();
                            return null;
                        }catch(SQLException se){
                            conn.rollback();
                            throw se;
                        }finally{
                            conn.close();
                        }
                    }
                }));
            }
            //wait for everyone to finish
            for(Future<Void> future:futures){
                future.get();
            }
            return new InsertResult(null);
        }finally{
            dataInserter.shutdown();
        }
    }

    public int getInsertBatchSize(){
        return insertBatch;
    }

    public String getInsertDataString(){
        StringBuilder ib = new StringBuilder("insert into ").append(name).append("(");
        boolean isStart=true;
        for(Column column:columns){
            if(!isStart)ib = ib.append(",");
            else isStart=false;
            ib = ib.append(column.getName());
        }
        ib = ib.append(") values (");
        isStart=true;
        //noinspection ForLoopReplaceableByForEach
        for(int i=0;i<columns.size();i++){
            if(!isStart)ib = ib.append(",");
            else isStart=false;
            ib = ib.append("?");
        }
        ib = ib.append(")");
        return ib.toString();
    }

    public String getCreateTableString(){
        StringBuilder cb = new StringBuilder("create table ").append(name).append("(");
        boolean isStart = true;
        List<Column> pkCols = Lists.newArrayList();
        for(Column column:columns){
            if(!isStart)cb = cb.append(",");
            else isStart = false;
            cb.append(column.getSqlRep());

            if(column.isPrimaryKey())pkCols.add(column);
        }

        if(pkCols.size()>0){
            cb = cb.append(", PRIMARY KEY(");
            isStart=true;
            for(Column pkCol:pkCols){
                if(!isStart)cb.append(",");
                else isStart=false;
                cb = cb.append(pkCol.getName());
            }
            cb = cb.append(")");
        }
        cb.append(")");
        return cb.toString();
    }

    @Override
    public String toString() {
        return "Table{" +
                "name='" + name + '\'' +
                ", columns=" + columns +
                ", numRows=" + numRows +
                '}';
    }

    public String getName() {
        return name;
    }

    public int getNumRows() {
        return numRows;
    }

    public void fillStatement(PreparedStatement ps) throws SQLException {
        int pos=1;
        for(Column column:columns){
            column.setData(ps, pos);
            pos++;
        }
    }

    public class InsertResult implements Result{
        private final Stats stats;

        public InsertResult(Stats stats) {
            this.stats = stats;
        }

        @Override
        public void write(PrintStream stream) throws Exception {
            stream.printf("Insertion stats:%n");
            stream.printf("%-25s%n",name);
            stream.printf("\t%-25s\t%15d threads%n","Number of threads",insertThreads);
            stream.printf("\t%-25s\t%15d records%n","Insert batch size",insertBatch);
            stream.printf("\t%-25s\t%15d samples%n","Number of samples",numRows/insertBatch);
            stream.printf("\t%-25s\t%15d records%n","Total records inserted", stats.getTotalRecords());
            stream.printf("\t%-25s\t%20.4f ms%n","Total time spent", DisplayTime.NANOSECONDS.toMillis(stats.getTotalTime()));
            stream.printf("\t%-25s\t%20.4f records/sec%n","Overall throughput",stats.getTotalRecords()/DisplayTime.NANOSECONDS.toSeconds(stats.getTotalTime()));
            stream.printf("--------------------TIME DISTRIBUTION--------------------%n");
            stream.printf("Per %d records:%n",insertBatch);
            stream.printf("\t%-20s\t%20.4f ms%n","min",DisplayTime.NANOSECONDS.toMillis(stats.getMinTime()));
            stream.printf("\t%-20s\t%20.4f ms%n","median(p50)",DisplayTime.NANOSECONDS.toMillis(stats.getMedian()));
            stream.printf("\t%-20s\t%20.4f ms%n","p75",DisplayTime.NANOSECONDS.toMillis(stats.get75P()));
            stream.printf("\t%-20s\t%20.4f ms%n","p95",DisplayTime.NANOSECONDS.toMillis(stats.get95P()));
            stream.printf("\t%-20s\t%20.4f ms%n","p98",DisplayTime.NANOSECONDS.toMillis(stats.get98P()));
            stream.printf("\t%-20s\t%20.4f ms%n","p99",DisplayTime.NANOSECONDS.toMillis(stats.get99P()));
            stream.printf("\t%-20s\t%20.4f ms%n","p999",DisplayTime.NANOSECONDS.toMillis(stats.get999P()));
            stream.printf("\t%-20s\t%20.4f ms%n","max",DisplayTime.NANOSECONDS.toMillis(stats.getMaxTime()));
            stream.println();
            stream.printf("\t%-20s\t%20.4f ms%n", "avg", DisplayTime.NANOSECONDS.toMillis(stats.getAvgTime()));
            stream.printf("\t%-20s\t%20.4f ms%n","std. dev",DisplayTime.NANOSECONDS.toMillis(stats.getTimeStandardDeviation()));
            stream.println();
            stream.printf("Per record:%n ");
            stream.printf("\t%-20s\t%20.4f ms%n","min",DisplayTime.NANOSECONDS.toMillis(stats.getMinTime()/insertBatch));
            stream.printf("\t%-20s\t%20.4f ms%n","median(p50)",DisplayTime.NANOSECONDS.toMillis(stats.getMedian()/insertBatch));
            stream.printf("\t%-20s\t%20.4f ms%n","p75",DisplayTime.NANOSECONDS.toMillis(stats.get75P()/insertBatch));
            stream.printf("\t%-20s\t%20.4f ms%n","p95",DisplayTime.NANOSECONDS.toMillis(stats.get95P()/insertBatch));
            stream.printf("\t%-20s\t%20.4f ms%n","p98",DisplayTime.NANOSECONDS.toMillis(stats.get98P()/insertBatch));
            stream.printf("\t%-20s\t%20.4f ms%n","p99",DisplayTime.NANOSECONDS.toMillis(stats.get99P()/insertBatch));
            stream.printf("\t%-20s\t%20.4f ms%n","p999",DisplayTime.NANOSECONDS.toMillis(stats.get999P()/insertBatch));
            stream.printf("\t%-20s\t%20.4f ms%n","max",DisplayTime.NANOSECONDS.toMillis(stats.getMaxTime()/insertBatch));
            stream.println();
            stream.printf("\t%-20s\t%20.4f ms%n","avg",DisplayTime.NANOSECONDS.toMillis(stats.getAvgTime()/insertBatch));
            stream.printf("\t%-20s\t%20.4f ms%n","std. dev",DisplayTime.NANOSECONDS.toMillis(stats.getTimeStandardDeviation()/insertBatch));
            stream.println();
        }
    }

    private class ImportResult implements Result{
        private final Map<String,Long> filePaths = new HashMap<String, Long>();
        private long totalTime;

        public void addTime(String fileName, long nanos){
            this.filePaths.put(fileName,nanos);
        }

        public void finalize(long timeTaken){
            this.totalTime = timeTaken;
        }

        @Override
        public void write(PrintStream stream) throws Exception {
            stream.printf("Import stats:%n");
            stream.printf("%-25s%n",name);
            stream.printf("%-25s\t%15d files%n","Number Files",filePaths.size());
            stream.printf("%-25s\t%20.4f seconds%n", "Total time spent", DisplayTime.NANOSECONDS.toSeconds(totalTime));
            stream.printf("---------------FILE STATISTICS---------------%n");
            for(String file:filePaths.keySet()){
                stream.printf("%-100s\t%20.4f%n", file, DisplayTime.NANOSECONDS.toSeconds(filePaths.get(file)));
            }
        }
    }
}
