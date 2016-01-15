package com.splicemachine.test;

import org.apache.commons.cli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Scott Fines
 *         Date: 7/23/14
 */
public class SimpleJoinQueryRunner extends BaseQueryRunner {
    private static final String hashSql = "select t.a,t.b,t2.b,t2.c \n" +
            "from --SPLICE-PROPERTIES joinOrder=FIXED \n" +
            "t \n" +
            ",t2 --SPLICE-PROPERTIES joinStrategy=HASH \n" +
            "where t.b = t2.b";
    private static final String nljSql = "select t.a,t.b,t2.b,t2.c \n" +
            "from --SPLICE-PROPERTIES joinOrder=FIXED \n" +
            "t \n" +
            ",t2 --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP \n" +
            "where t.b = t2.b";
    private static final String bcastSql = "select t.a,t.b,t2.b,t2.c \n" +
            "from --SPLICE-PROPERTIES joinOrder=FIXED \n" +
            "t \n" +
            ",t2 --SPLICE-PROPERTIES joinStrategy=BROADCAST \n" +
            "where t.b = t2.b";

    private static final String sql = hashSql;
    @Override
    protected Runner newRunner(Connection jdbcConn, int numIterations, File outputDir, int threadId, CountDownLatch startLatch) throws IOException {
        return new Runner(jdbcConn,numIterations,outputDir,threadId,startLatch) {
            @Override
            protected void executeIteration(int iteration, PreparedStatement ps) throws SQLException, IOException {
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

    }

    public static void main(String...args) throws Exception{
        new SimpleJoinQueryRunner().run(args);
    }
}
