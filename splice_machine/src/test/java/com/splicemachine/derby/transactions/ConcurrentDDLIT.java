/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.transactions;

import org.spark_project.guava.collect.Lists;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SlowTest;
import com.splicemachine.test.Transactions;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
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
// Warning: creates hundreds of conglomerates
@Category({Transactions.class, SlowTest.class})
public class ConcurrentDDLIT {
    private static final Logger LOG = Logger.getLogger(ConcurrentDDLIT.class);
    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(ConcurrentDDLIT.class.getSimpleName().toUpperCase());

    public static final SpliceWatcher classWatcher = new SpliceWatcher();
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher);

    @Rule public final SpliceWatcher methodWatcher = new SpliceWatcher();

    private static final int nThreads = 4;
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
    public void testConcurrentShowTables() throws Exception {
        int numTables = 100;
        List<Future<Void>> results = Lists.newArrayListWithExpectedSize(nThreads);
        for(int i=0;i<nThreads;i++){
            TestConnection conn = connections.get(i);
            results.add(executor.submit(new ShowTablesCallable(i,conn,numTables)));
        }
        for(Future<Void> future:results){
            future.get(); //check for errors
        }
    }

    @Test(timeout = 1000000)
    public void testConcurrentSchemaCreation() throws Exception {
        int numTables = 100;
        List<Future<Void>> results = Lists.newArrayListWithExpectedSize(nThreads);
        for(int i=0;i<nThreads;i++){
            TestConnection conn = connections.get(i);
            results.add(executor.submit(new CreateSchemaCallable(i,conn,numTables)));
        }
        for(Future<Void> future:results){
            future.get(); //check for errors
        }
    }

    @Test(timeout = 1000000)
    public void testConcurrentMetadataFetch() throws Exception {
        int numTables = 100;
        List<Future<Void>> results = Lists.newArrayListWithExpectedSize(nThreads);
        for(int i=0;i<nThreads;i++){
            TestConnection conn = connections.get(i);
            results.add(executor.submit(new FetchMetadataCallable(i,conn,numTables)));
        }
        for(Future<Void> future:results){
            future.get(); //check for errors
        }
    }

    @Test(timeout = 1000000)
    public void testConcurrentConstrainedTableCreation() throws Exception {
        int numTables = 100;
        List<Future<Void>> results = Lists.newArrayListWithExpectedSize(nThreads);
        for(int i=0;i<nThreads;i++){
            TestConnection conn = connections.get(i);
            results.add(executor.submit(new CreateConstrainedTableCallable(i,conn,numTables)));
        }
        for(Future<Void> future:results){
            future.get(); //check for errors
        }
    }

    @Test(timeout = 1000000)
    public void testConcurrentTableCreation() throws Exception {
        int numTables = 100;
        List<Future<Void>> results = Lists.newArrayListWithExpectedSize(nThreads);
        for(int i=0;i<nThreads;i++){
            TestConnection conn = connections.get(i);
            results.add(executor.submit(new CreateTableCallable(i,conn,numTables)));
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
            results.add(executor.submit(new CreateIndexCallable(i,conn,numTables)));
        }
        for(Future<Void> future:results){
            future.get(); //check for errors
        }
    }

    private class FetchMetadataCallable extends CreateTableCallable {
        private ResultSet rs;

        protected FetchMetadataCallable(int position, TestConnection conn, int numElements) {
            super(position, conn, numElements);
        }

        @Override
        protected void setupAction(int value) throws SQLException {
            rs = conn.getMetaData().getTables(null,schemaWatcher.schemaName,getRawTableName(value),null);
        }

        @Override
        protected void teardownAction(int value) throws SQLException {
            if(rs.next()){
                super.teardownAction(value);
            }
        }
    }

    private class CreateSchemaCallable extends Action {

        protected CreateSchemaCallable(int position, TestConnection conn, int numElements) {
            super(position, conn, numElements);
        }

        @Override
        protected void setupAction(int value) throws SQLException {
            String schema = schemaWatcher.schemaName+"_t_"+value;
            conn.createStatement().execute("create schema " + schema);
        }

        @Override
        protected void teardownAction(int value) throws SQLException {
            String schema = schemaWatcher.schemaName+"_t_"+value;
            conn.createStatement().execute("drop schema "+schema+ " restrict");
        }
    }

    private class CreateConstrainedTableCallable extends CreateTableCallable{

        private CreateConstrainedTableCallable(int position, TestConnection conn, int numElements) {
            super(position, conn, numElements);
        }

        @Override
        protected void setupAction(int value) throws SQLException {
            String table = getTableName(value);
            conn.createStatement().execute("create table "+ table+" (a int UNIQUE not null,b int)");
        }

        @Override
        protected void teardownAction(int value) throws SQLException {
            conn.createStatement().execute("drop table "+ getTableName(value));
        }
    }
    private class CreateTableCallable extends Action{

        protected CreateTableCallable(int position, TestConnection conn, int numElements) {
            super(position, conn, numElements);
        }

        @Override
        protected void setupAction(int value) throws SQLException {
            String table = getTableName(value);
            conn.createStatement().execute("create table " + table + "(a int, b int)");
        }

        @Override
        protected void teardownAction(int value) throws SQLException {
            String table = getTableName(value);
            conn.createStatement().execute("drop table "+table);
        }

        protected String getTableName(int value) {
            String schema = schemaWatcher.schemaName;
            return schema + "." + getRawTableName(value);
        }

        protected String getRawTableName(int value) {
            return "t" + value;
        }
    }

    private abstract class Action implements Callable<Void>{
        protected final int position;
        protected final TestConnection conn;
        protected final int numElements;

        protected Action(int position, TestConnection conn, int numElements) {
            this.position = position;
            this.conn = conn;
            this.numElements = numElements;
        }

        @Override
        public Void call() throws Exception {
            LOG.info("Beginning Create/Drop test on position "+ position);
            int value = position*numElements;
            for(int i=value;i<value+numElements;i++){
                long txnId;
                try{
                    txnId = conn.getCurrentTransactionId();
                    //LOG.debug("Performing setup with txnId "+txnId);
                    conn.clearWarnings();
                    setupAction(i);
                    printWarnings();
                    conn.commit();
                }catch(SQLException se){
                    LOG.error("Failed setup action ",se);
                    throw se;
                }


                try{
                    txnId = conn.getCurrentTransactionId();
                    //LOG.debug("Performing teardown with txnId "+txnId);
                    teardownAction(i);
                    printWarnings();
                    conn.commit();
                }catch(SQLException se){
                    LOG.error("Failed teardown action ",se);
                    throw se;
                }
            }
            return null;
        }

        protected abstract void setupAction(int value) throws SQLException;

        protected abstract void teardownAction(int value) throws SQLException;

        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        private void printWarnings() throws SQLException {
            SQLWarning warnings = conn.getWarnings();
            if(warnings!=null){
                LOG.debug("Warning on create index: " + warnings.getNextWarning());
            }
        }

    }

    private class CreateIndexCallable extends CreateTableCallable{

        protected CreateIndexCallable(int position, TestConnection conn, int numElements) {
            super(position, conn, numElements);
        }

        @Override
        protected void setupAction(int value) throws SQLException {
            String index = schemaWatcher.schemaName+".t_idx_"+value;
            super.setupAction(value);
            conn.createStatement().execute("create index " + index + " on " + getTableName(value) + "(a)");

        }

        @Override
        protected void teardownAction(int value) throws SQLException {
            String index = schemaWatcher.schemaName+".t_idx_"+value;
            conn.createStatement().execute("drop index " + index);
            super.teardownAction(value);
        }
    }

    private class ShowTablesCallable extends Action {
        private ShowTablesCallable(int position, TestConnection conn, int numElements) {
            super(position, conn, numElements);
        }

        @Override
        protected void setupAction(int value) throws SQLException {
            conn.setAutoCommit(false);
            conn.getMetaData().getTables(null,null,"SYSTABLES",null);
            conn.commit();
        }

        @Override protected void teardownAction(int value) throws SQLException {  }
    }
}
