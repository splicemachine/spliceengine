package com.splicemachine.perf.runner;

import com.google.common.collect.Lists;
import com.splicemachine.derby.stats.Accumulator;
import com.splicemachine.derby.stats.Stats;
import com.splicemachine.derby.stats.TimeUtils;
import com.splicemachine.derby.stats.TimingStats;
import com.splicemachine.perf.runner.qualifiers.Result;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

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

    public Table(String name, List<Column> columns, int numRows, int insertBatch, int insertThreads) {
        this.name = name;
        this.columns = columns;
        this.numRows = numRows;
        this.insertBatch = insertBatch;
        this.insertThreads = insertThreads;
    }

    public void create(Connection conn) throws SQLException{
        SpliceLogUtils.trace(LOG, "Creating table %s",name);
        PreparedStatement ps = conn.prepareStatement(getCreateTableString());
        ps.execute();
    }

    public Result insertData(final JDBCConnectionPool connectionPool) throws Exception{
        ExecutorService dataInserter = Executors.newFixedThreadPool(insertThreads);
        try{
            List<Future<Void>> futures = Lists.newArrayListWithCapacity(insertThreads+1);
            final Accumulator insertAccumulator = TimingStats.uniformSafeAccumulator();
            insertAccumulator.start();
            final AtomicInteger numBatches = new AtomicInteger(0);
            for(int i=0;i<insertThreads;i++){
                futures.add(dataInserter.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        Connection conn = connectionPool.getConnection();
                        try{
                            PreparedStatement ps = conn.prepareStatement(getInsertDataString());
                            for(int numRowsInserted=0;numRowsInserted<numRows/insertThreads;numRowsInserted++){
                                fillStatement(ps);
                                ps.addBatch();
                                if(numRowsInserted%insertBatch==0){
                                    long start = System.nanoTime();
                                    int numInserted = ps.executeBatch().length;
                                    insertAccumulator.tick(numInserted,System.nanoTime()-start);
                                    int batches = numBatches.incrementAndGet();
                                    if(batches%100==0)
                                        SpliceLogUtils.info(LOG,"inserted %d batches",batches);
                                }
                            }
                            long start = System.nanoTime();
                            int numInserted = ps.executeBatch().length;
                            if(numInserted>0){
                                insertAccumulator.tick(numInserted,System.nanoTime()-start);
                                numBatches.incrementAndGet();
                            }
                            conn.commit();
                            return null;
                        }catch(SQLException se){
                            conn.rollback();
                            throw se;
                        }finally{
                            connectionPool.returnConnection(conn);
                        }
                    }
                }));
            }
            //wait for everyone to finish
            for(Future<Void> future:futures){
                future.get();
            }
            return new InsertResult(insertAccumulator.finish());
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
            stream.printf("\t%-25s\t%20.4f ms%n","Total time spent", TimeUtils.toMillis(stats.getTotalTime()));
            stream.printf("\t%-25s\t%20.4f records/sec%n","Overall throughput",stats.getTotalRecords()/TimeUtils.toSeconds(stats.getTotalTime()));
            stream.printf("--------------------TIME DISTRIBUTION--------------------%n");
            stream.printf("Per %d records:%n",insertBatch);
            stream.printf("\t%-20s\t%20.4f ms%n","min",TimeUtils.toMillis(stats.getMinTime()));
            stream.printf("\t%-20s\t%20.4f ms%n","median(p50)",TimeUtils.toMillis(stats.getMedian()));
            stream.printf("\t%-20s\t%20.4f ms%n","p75",TimeUtils.toMillis(stats.get75P()));
            stream.printf("\t%-20s\t%20.4f ms%n","p95",TimeUtils.toMillis(stats.get95P()));
            stream.printf("\t%-20s\t%20.4f ms%n","p98",TimeUtils.toMillis(stats.get98P()));
            stream.printf("\t%-20s\t%20.4f ms%n","p99",TimeUtils.toMillis(stats.get99P()));
            stream.printf("\t%-20s\t%20.4f ms%n","p999",TimeUtils.toMillis(stats.get999P()));
            stream.printf("\t%-20s\t%20.4f ms%n","max",TimeUtils.toMillis(stats.getMaxTime()));
            stream.println();
            stream.printf("\t%-20s\t%20.4f ms%n", "avg", TimeUtils.toMillis(stats.getAvgTime()));
            stream.printf("\t%-20s\t%20.4f ms%n","std. dev",TimeUtils.toMillis(stats.getTimeStandardDeviation()));
            stream.println();
            stream.printf("Per record:%n ");
            stream.printf("\t%-20s\t%20.4f ms%n","min",TimeUtils.toMillis(stats.getMinTime()/insertBatch));
            stream.printf("\t%-20s\t%20.4f ms%n","median(p50)",TimeUtils.toMillis(stats.getMedian()/insertBatch));
            stream.printf("\t%-20s\t%20.4f ms%n","p75",TimeUtils.toMillis(stats.get75P()/insertBatch));
            stream.printf("\t%-20s\t%20.4f ms%n","p95",TimeUtils.toMillis(stats.get95P()/insertBatch));
            stream.printf("\t%-20s\t%20.4f ms%n","p98",TimeUtils.toMillis(stats.get98P()/insertBatch));
            stream.printf("\t%-20s\t%20.4f ms%n","p99",TimeUtils.toMillis(stats.get99P()/insertBatch));
            stream.printf("\t%-20s\t%20.4f ms%n","p999",TimeUtils.toMillis(stats.get999P()/insertBatch));
            stream.printf("\t%-20s\t%20.4f ms%n","max",TimeUtils.toMillis(stats.getMaxTime()/insertBatch));
            stream.println();
            stream.printf("\t%-20s\t%20.4f ms%n","avg",TimeUtils.toMillis(stats.getAvgTime()/insertBatch));
            stream.printf("\t%-20s\t%20.4f ms%n","std. dev",TimeUtils.toMillis(stats.getTimeStandardDeviation()/insertBatch));
            stream.println();
        }
    }
}
