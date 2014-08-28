package com.splicemachine.derby.transactions;

import com.google.common.collect.Lists;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Tests that we can perform concurrent DDL operations.
 *
 * @author Scott Fines
 * Date: 8/28/14
 */
@Ignore("Creates hundreds of conglomerates")
public class ConcurrentDDLIT {
    private static final Logger LOG = Logger.getLogger(ConcurrentDDLIT.class);
    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(ConcurrentDDLIT.class.getSimpleName().toUpperCase());

    public static final SpliceWatcher classWatcher = new SpliceWatcher();
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher);

    @Rule public final SpliceWatcher methodWatcher = new SpliceWatcher();

    private static final int nThreads = 2;
    private ExecutorService executor;
    private static List<TestConnection> connections;

    @BeforeClass
    public static void setUpClass() throws Exception {
        connections = Lists.newArrayListWithExpectedSize(nThreads);
        for(int i=0;i<nThreads;i++){
            TestConnection connection = classWatcher.createConnection();
            connection.setAutoCommit(false);
            connections.add(connection);
        }
    }

    @Before
    public void setUp() throws Exception {
        executor = Executors.newFixedThreadPool(nThreads);
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();
        for(TestConnection conn:connections){
            conn.rollback();
            conn.reset();
        }
    }

    @Test(timeout = 1000000)
    public void testConcurrentTableCreation() throws Exception {
        int numTables = 100;
        List<Future<Void>> results = Lists.newArrayListWithExpectedSize(nThreads);
        for(int i=0;i<nThreads;i++){
            TestConnection conn = connections.get(i);
            results.add(executor.submit(new CreateTableCallable(i,numTables,conn)));
        }
        for(Future<Void> future:results){
            future.get(); //check for errors
        }
    }

    @Test(timeout = 1000000)
    public void testConcurrentIndexCreation() throws Exception {
        int numTables = 100;
        List<Future<Void>> results = Lists.newArrayListWithExpectedSize(nThreads);
        for(int i=0;i<nThreads;i++){
            TestConnection conn = connections.get(i);
            results.add(executor.submit(new CreateIndexCallable(i,numTables,conn)));
        }
        for(Future<Void> future:results){
            future.get(); //check for errors
        }
    }

    private class CreateTableCallable implements Callable<Void> {
        private final int position;
        private final TestConnection conn;
        private final int numElements;

        public CreateTableCallable(int position,int numElements, TestConnection conn) {
            this.position = position;
            this.conn = conn;
            this.numElements = numElements;
        }

        @Override
        public Void call() throws Exception {
            LOG.info("Beginning Create/Drop test on position "+ position);
            int value = position*numElements;
            for(int i=value;i<value+numElements;i++){
                String table = schemaWatcher.schemaName+".t"+i;
                long txnId;
                try{
                    txnId = conn.getCurrentTransactionId();
                    SpliceLogUtils.debug(LOG,"Creating table %s with txnId %d",table,txnId);
                    conn.createStatement().execute("create table " + table + "(a int, b int)");
                    conn.commit();
                }catch(SQLException se){
                    LOG.error("Failed to create table "+ table,se);
                    throw se;
                }
                try{
                    txnId = conn.getCurrentTransactionId();
                    SpliceLogUtils.debug(LOG,"Dropping table %s with txnId %d",table,txnId);
                    conn.createStatement().execute("drop table "+table);
                    conn.commit();
                }catch(SQLException se){
                    LOG.error("Failed to drop table "+ table,se);
                    throw se;
                }
            }
            return null;
        }
    }


    private class CreateIndexCallable implements Callable<Void> {
        private final int position;
        private final TestConnection conn;
        private final int numElements;

        public CreateIndexCallable(int position,int numElements, TestConnection conn) {
            this.position = position;
            this.conn = conn;
            this.numElements = numElements;
        }

        @Override
        public Void call() throws Exception {
            LOG.info("Beginning Create/Drop test on position "+ position);
            int value = position*numElements;
            for(int i=value;i<value+numElements;i++){
                String table = schemaWatcher.schemaName+".t"+i;
                long txnId;
                try{
                    txnId = conn.getCurrentTransactionId();
                    SpliceLogUtils.debug(LOG,"Creating table %s with txnId %d",table,txnId);
                    conn.clearWarnings();
                    conn.createStatement().execute("create table " + table + "(a int, b int)");
                    conn.commit();
                }catch(SQLException se){
                    LOG.error("Failed to create table "+ table,se);
                    throw se;
                }
                String index = schemaWatcher.schemaName+".t_idx_"+i;
                try{
                    txnId = conn.getCurrentTransactionId();
                    SpliceLogUtils.debug(LOG,"Creating index %s with txnId %d",table,txnId);
                    conn.clearWarnings();
                    conn.createStatement().execute("create index " + index + " on " + table + "(a)");
                    SQLWarning warnings = conn.getWarnings();
                    if(warnings!=null){
                        SpliceLogUtils.debug(LOG,"Warning on create index: " + warnings.getNextWarning());
                    }
                    conn.commit();
                }catch(SQLException se){
                    LOG.error("Failed to create index "+ index,se);
                    throw se;
                }

                try{
                    txnId = conn.getCurrentTransactionId();
                    SpliceLogUtils.debug(LOG,"Dropping index %s with txnId %d",index,txnId);
                    conn.createStatement().execute("drop index " + index);
                    conn.commit();
                }catch(SQLException se){
                    LOG.error("Failed to drop index "+ index,se);
                    throw se;
                }

                try{
                    txnId = conn.getCurrentTransactionId();
                    SpliceLogUtils.debug(LOG,"Dropping table %s with txnId %d",table,txnId);
                    conn.createStatement().execute("drop table "+table);
                    conn.commit();
                }catch(SQLException se){
                    LOG.error("Failed to drop table "+ table,se);
                    throw se;
                }
            }
            return null;
        }
    }

}
