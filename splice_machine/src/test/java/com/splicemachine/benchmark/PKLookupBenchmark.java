/*
 * Copyright (c) 2021 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.benchmark;

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.test.Benchmark;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.*;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

@Category(Benchmark.class)
public class PKLookupBenchmark extends Benchmark{

    private static final Logger LOG = org.apache.logging.log4j.LogManager.getLogger(PKLookupBenchmark.class);

    private static final int DEFAULT_CONNECTIONS = 10;
    private static final int DEFAULT_OPS = 10000;
    static final int DEFAULT_WAREHOUSES = 100;

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(PKLookupBenchmark.class.getSimpleName());

    static Connection makeConnection() throws SQLException {
        Connection connection = SpliceNetConnection.getDefaultConnection();
        connection.setSchema(spliceSchemaWatcher.schemaName);
        connection.setAutoCommit(true);
        return connection;
    }

    static Connection testConnection;
    static Statement testStatement;
    static int warehouses = DEFAULT_WAREHOUSES;
    static int districts = TPCCLoader.configDistPerWhse;
    static int customers = TPCCLoader.configCustPerDist;

    @BeforeClass
    public static void setUp() throws Exception {
        getInfo();

        LOG.info("Create tables");

        TPCCLoader loader = new TPCCLoader(spliceSchemaWatcher.schemaName, warehouses);
        loader.loadWarehouses();
        loader.loadCustomers();

        testConnection = makeConnection();
        testStatement = testConnection.createStatement();

        LOG.info("Analyze");
        testStatement.execute("ANALYZE SCHEMA " + spliceSchemaWatcher.schemaName);
    }

    static final String STAT_ERROR = "ERROR";
    static final String STAT_GET_CUST = "GET_PK";
    static final String STAT_GET_WHS = "GET_PK2";
    static final String STAT_GET_CUSTWHS = "GET_COMBINE";
    static final String STAT_GET_JOIN = "GET_JOIN";

    private static void doSeparateLookups(int operations) {
        try (Connection conn = makeConnection()) {
            PreparedStatement sqlGetCustomer  = conn.prepareStatement("SELECT c_discount, c_last, c_credit FROM " + TPCCLoader.TABLE_CUSTOMER + " WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
            PreparedStatement sqlGetWarehouse = conn.prepareStatement("SELECT w_tax FROM " + TPCCLoader.TABLE_WAREHOUSE + " WHERE w_id = ?");
            Random rnd = ThreadLocalRandom.current();

            for (int i = 0; i < operations; ++i) {
                int warehouse = rnd.nextInt(warehouses) + 1;
                int district  = rnd.nextInt(districts) + 1;
                int customer  = rnd.nextInt(customers) + 1;

                sqlGetCustomer.setInt(1, warehouse);
                sqlGetCustomer.setInt(2, district);
                sqlGetCustomer.setInt(3, customer);

                long start = System.currentTimeMillis();
                try (ResultSet rs = sqlGetCustomer.executeQuery()) {
                    updateStats(STAT_GET_CUST, System.currentTimeMillis() - start);

                    if (!rs.next()) {
                        updateStats(STAT_ERROR);
                    } else {
                        rs.getFloat("c_discount");
                        rs.getString("c_last");
                        rs.getString("c_credit");
                    }
                }

                sqlGetWarehouse.setInt(1, warehouse);
                start = System.currentTimeMillis();
                try (ResultSet rs = sqlGetWarehouse.executeQuery()) {
                    updateStats(STAT_GET_WHS, System.currentTimeMillis() - start);

                    if (!rs.next()) {
                        updateStats(STAT_ERROR);
                    } else {
                        rs.getFloat("w_tax");
                    }
                }
            }
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }

    private static void doCombinedLookups(int operations) {
        try (Connection conn = makeConnection()) {
            PreparedStatement sqlGetCustWhs  = conn.prepareStatement(
                    "SELECT c_discount, c_last, c_credit, w_tax  FROM " + TPCCLoader.TABLE_CUSTOMER + ", " + TPCCLoader.TABLE_WAREHOUSE
                            + " WHERE w_id = ? AND c_w_id = ? AND c_d_id = ? AND c_id = ?");
            Random rnd = ThreadLocalRandom.current();

            for (int i = 0; i < operations; ++i) {
                int warehouse = rnd.nextInt(warehouses) + 1;
                int district  = rnd.nextInt(districts) + 1;
                int customer  = rnd.nextInt(customers) + 1;

                sqlGetCustWhs.setInt(1, warehouse);
                sqlGetCustWhs.setInt(2, warehouse);
                sqlGetCustWhs.setInt(3, district);
                sqlGetCustWhs.setInt(4, customer);

                long start = System.currentTimeMillis();
                try (ResultSet rs = sqlGetCustWhs.executeQuery()) {
                    updateStats(STAT_GET_CUSTWHS, System.currentTimeMillis() - start);

                    if (!rs.next()) {
                        updateStats(STAT_ERROR);
                    } else {
                        rs.getFloat("c_discount");
                        rs.getString("c_last");
                        rs.getString("c_credit");
                        rs.getFloat("w_tax");
                    }
                }
            }
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }

    private static void doJoinedLookups(int operations) {
        try (Connection conn = makeConnection()) {
            PreparedStatement sqlGetCustWhs  = conn.prepareStatement(
                    "SELECT c_discount, c_last, c_credit, w_tax  FROM " + TPCCLoader.TABLE_WAREHOUSE + " JOIN " + TPCCLoader.TABLE_CUSTOMER
                            + " ON (w_id = c_w_id) WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
            Random rnd = ThreadLocalRandom.current();

            for (int i = 0; i < operations; ++i) {
                int warehouse = rnd.nextInt(warehouses) + 1;
                int district  = rnd.nextInt(districts) + 1;
                int customer  = rnd.nextInt(customers) + 1;

                sqlGetCustWhs.setInt(1, warehouse);
                sqlGetCustWhs.setInt(2, district);
                sqlGetCustWhs.setInt(3, customer);

                long start = System.currentTimeMillis();
                try (ResultSet rs = sqlGetCustWhs.executeQuery()) {
                    updateStats(STAT_GET_JOIN, System.currentTimeMillis() - start);

                    if (!rs.next()) {
                        updateStats(STAT_ERROR);
                    } else {
                        rs.getFloat("c_discount");
                        rs.getString("c_last");
                        rs.getString("c_credit");
                        rs.getFloat("w_tax");
                    }
                }
            }
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }

    @Test
    public void separateLookupsSingle() throws Exception {
        LOG.info("separateLookupsSingle");
        runBenchmark(1, () -> doSeparateLookups(DEFAULT_OPS));
    }

    @Test
    public void separateLookupsMulti() throws Exception {
        LOG.info("separateLookupsMulti");
        runBenchmark(DEFAULT_CONNECTIONS, () -> doSeparateLookups(DEFAULT_OPS));
    }

    @Test
    public void combinedLookupsSingle() throws Exception {
        LOG.info("combinedLookupsSingle");
        runBenchmark(1, () -> doCombinedLookups(DEFAULT_OPS));
    }

    @Test
    public void combinedLookupsMulti() throws Exception {
        LOG.info("combinedLookupsMulti");
        runBenchmark(DEFAULT_CONNECTIONS, () -> doCombinedLookups(DEFAULT_OPS));
    }

    @Test
    public void joinedLookupsSingle() throws Exception {
        LOG.info("joinedLookupsSingle");
        runBenchmark(1, () -> doJoinedLookups(DEFAULT_OPS));
    }

    @Test
    public void joinedLookupsMulti() throws Exception {
        LOG.info("joinedLookupsMulti");
        runBenchmark(DEFAULT_CONNECTIONS, () -> doJoinedLookups(DEFAULT_OPS));
    }
}
