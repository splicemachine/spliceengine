package com.splicemachine.test;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Scott Fines
 *         Date: 6/6/14
 */
public class CommandLineQueryRunner extends BaseQueryRunner{
    private String sql;
    @Override
    protected Options getOptions() {
        Options options = super.getOptions();
        Option sql = new Option("s","sql",true,"The Sql to execute, surrounded in quotes");
        sql.setRequired(true);
        options.addOption(sql);
        return options;
    }

    @Override
    protected BaseQueryRunner.Runner newRunner(Connection jdbcConn, int numIterations, File outputDir, int threadId, CountDownLatch startLatch) throws IOException {
        return new Runner(jdbcConn,numIterations,outputDir,threadId,startLatch) {
            @Override
            protected void executeIteration(int iteration,PreparedStatement ps) throws SQLException, IOException {
                ResultSet rs = ps.executeQuery();
                long size = 0;
                while(rs.next()){
                    size++;
                }
                reportSize(iteration,size);
            }

            @Override
            protected PreparedStatement getPreparedStatement() throws Exception {
                return connection.prepareStatement(sql);
            }
        };
    }

    @Override
    protected void parseAdditionalOptions(CommandLine cli) throws Exception {
        sql = cli.getOptionValue("s");
    }

    public static void main(String...args) throws Exception{
        new CommandLineQueryRunner().run(args);
    }
}
