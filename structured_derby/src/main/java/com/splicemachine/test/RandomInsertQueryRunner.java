package com.splicemachine.test;

import com.google.common.collect.Lists;
import com.splicemachine.derby.utils.ErrorState;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.hbase.util.Pair;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * QueryRunner that randomly inserts data into a table with the schema
 *
 * (a int, b int, primary key (a)).
 *
 * This is useful for raw primary key benchmarks
 * @author Scott Fines
 * Date: 9/5/14
 */
public class RandomInsertQueryRunner extends BaseQueryRunner{
    private String sql = "insert into %s (a,b) values (?,?)";

    private String tableName;

    private Queue<Pair<Integer,Integer>> ranges;

    @Override
    protected Options getOptions() {
        Options options = super.getOptions();
        Option sql = new Option("s","table-name",true,
                "The name of the table to load data into(including schema, if it's not in the default)");
        sql.setRequired(true);
        options.addOption(sql);

        Option rangeStart = new Option("r","range-start",true,"The start of the insertion range");
        rangeStart.setRequired(true);
        options.addOption(rangeStart);

        Option rangeEnd = new Option("w","range-end",true,"The end of the insertion range");
        rangeEnd.setRequired(true);
        options.addOption(rangeEnd);

        return options;
    }

    @Override
    protected Runner newRunner(Connection jdbcConn, int numIterations, File outputDir, int threadId, CountDownLatch startLatch) throws IOException {
        Pair<Integer,Integer> range = ranges.poll(); //take a range off the list
        final int start = range.getFirst();
        final int stop = range.getSecond();
        final AtomicInteger counter = new AtomicInteger(start);
        return new Runner(jdbcConn,numIterations,outputDir,threadId,startLatch) {
            @Override
            protected void executeIteration(int iteration, PreparedStatement ps) throws SQLException, IOException {
                int pk = counter.incrementAndGet();
                ps.setInt(1,pk);ps.setInt(2,pk);
                int update = ps.executeUpdate();
                reportSize(iteration,update);
            }

            @Override
            protected PreparedStatement getPreparedStatement() throws Exception {
                return connection.prepareStatement(String.format(sql,tableName));
            }
        };
    }

    @Override
    protected void parseAdditionalOptions(CommandLine cli) throws Exception {
        tableName = cli.getOptionValue("s");
        int rangeStart = Integer.parseInt(cli.getOptionValue("r"));
        int rangeEnd = Integer.parseInt(cli.getOptionValue("w"));
        int numThreads = Integer.parseInt(cli.getOptionValue("t"));

        int rangeSizePerThread = (rangeEnd-rangeStart)/numThreads;
        ranges = Lists.newLinkedList();
        int start  = rangeStart;
        int end = rangeStart+rangeSizePerThread;
        for(int i=0;i<numThreads;i++){
            ranges.add(Pair.newPair(start,end));
            start = end;
            end +=rangeSizePerThread;
        }

    }

    public static void main(String...args) throws Exception{
        new RandomInsertQueryRunner().run(args);
    }
}
