package com.splicemachine.benchmark;

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.test.Benchmark;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class TPCCLoader {

    private static final Logger LOG = Logger.getLogger(TPCCLoader.class);

    public static final String TABLE_WAREHOUSE = "WAREHOUSE";
    public static final String TABLE_CUSTOMER = "CUSTOMER";
    public static final int configDistPerWhse = 10;
    public static final int configCustPerDist = 3000;

    private String schema;
    private int warehouses;

    public TPCCLoader(String schema, int scaleFactor) {
        this.schema = schema;
        this.warehouses = scaleFactor;
    }

    private Connection makeConnection() throws SQLException {
        Connection connection = SpliceNetConnection.getDefaultConnection();
        connection.setSchema(schema);
        connection.setAutoCommit(true);
        return connection;
    }

    static AtomicInteger taskId = new AtomicInteger(0);

    private int nextWarehouse() {
        int warehouse = 1 + taskId.getAndIncrement();
        return warehouse <= warehouses ? warehouse : 0;
    }

    private void loadWarehouse() {
        try (Connection conn = makeConnection()) {
            try (PreparedStatement statement = conn.prepareStatement("INSERT INTO " + TABLE_WAREHOUSE + " VALUES(?,?,?,?,?,?,?,?,?)")) {

                for (; ; ) {
                    int w = nextWarehouse();
                    if (w <= 0) break;
                    Random random = new Random(w + "loadWarehouse".hashCode());

                    long start = System.currentTimeMillis();
                    statement.setLong(1, w);                                                       // w_id
                    statement.setDouble(2, 300000d);                                               // w_ytd
                    statement.setDouble(3, TPCCUtil.randomNumber(0, 2000, random) / 10000.0f);     // w_tax
                    statement.setString(4, TPCCUtil.randomStr(6, 10, random));                     // w_name
                    statement.setString(5, TPCCUtil.randomStr(10, 20, random));                    // w_street_1
                    statement.setString(6, TPCCUtil.randomStr(10, 20, random));                    // w_street_2
                    statement.setString(7, TPCCUtil.randomStr(10, 20, random));                    // w_city
                    statement.setString(8, TPCCUtil.randomStr(2, random).toUpperCase());           // w_state
                    // TPC-C 4.3.2.7: 4 random digits + "11111"
                    statement.setString(9, TPCCUtil.randomNStr(4, random) + "11111");              // w_zip
                    statement.execute();
                    Benchmark.updateStats("WAREHOUSES", System.currentTimeMillis() - start);
                }
            }
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }

    public void loadWarehouses() throws SQLException {
        try (Connection connection = makeConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + TABLE_WAREHOUSE + "(" +
                        "    w_id int NOT NULL,               " +
                        "    w_ytd decimal(12,2) NOT NULL,    " +
                        "    w_tax decimal(4,4) NOT NULL,     " +
                        "    w_name varchar(10) NOT NULL,     " +
                        "    w_street_1 varchar(20) NOT NULL, " +
                        "    w_street_2 varchar(20) NOT NULL, " +
                        "    w_city varchar(20) NOT NULL,     " +
                        "    w_state char(2) NOT NULL,        " +
                        "    w_zip char(9) NOT NULL,          " +
                        "    PRIMARY KEY (w_id)               " +
                        ")");
            }
        }

        taskId.set(0);
        Benchmark.runBenchmark(10, this::loadWarehouse);
    }


    private void loadCustomer() {
        try (Connection conn = makeConnection()) {
            try (PreparedStatement statement = conn.prepareStatement("INSERT INTO " + TABLE_CUSTOMER + " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")) {

                for (; ; ) {
                    int w = nextWarehouse();
                    if (w <= 0) break;
                    Random random = new Random(w + "loadCustomer".hashCode());

                    long start = System.currentTimeMillis();
                    for (int d = 1; d <= configDistPerWhse; d++) {
                        for (int c = 1; c <= configCustPerDist; c++) {
                            statement.setLong(1, w);                                                     // c_w_id
                            statement.setLong(2, d);                                                     // c_d_id
                            statement.setLong(3, c);                                                     // c_id
                            statement.setDouble(4, TPCCUtil.randomNumber(1, 5000, random) / 10000.0d);   // c_discount
                            boolean badCredit = TPCCUtil.randomNumber(1, 100, random) <= 10;
                            statement.setString(5, badCredit ? "BC" : "GC");                             // c_credit
                            String c_last = c <= 1000 ? TPCCUtil.getLastName(c - 1) :
                                    TPCCUtil.getNonUniformRandomLastNameForLoad(random);
                            statement.setString(6, c_last);                                              // c_last
                            statement.setString(7, TPCCUtil.randomStr(8, 16, random));                   // c_first
                            statement.setDouble(8, 50000d);                                              // c_credit_lim
                            statement.setDouble(9, -10d);                                                // c_balance
                            statement.setDouble(10, 10d);                                                // c_ytd_payment
                            statement.setLong(11, 1);                                                    // c_payment_cnt
                            statement.setLong(12, 0);                                                    // c_delivery_cnt
                            statement.setString(13, TPCCUtil.randomStr(10, 20, random));                 // c_street_1
                            statement.setString(14, TPCCUtil.randomStr(10, 20, random));                 // c_street_2
                            statement.setString(15, TPCCUtil.randomStr(10, 20, random));                 // c_city
                            statement.setString(16, TPCCUtil.randomStr(2, random).toUpperCase());        // c_state
                            statement.setString(17, TPCCUtil.randomNStr(4, random) + "11111");           // c_zip
                            statement.setString(18, TPCCUtil.randomNStr(16, random));                    // c_phone
                            Timestamp sysdate = new Timestamp(System.currentTimeMillis());
                            statement.setTimestamp(19, sysdate);                                         // c_since
                            statement.setString(20, "OE");                                               // c_middle
                            statement.setString(21, TPCCUtil.randomStr(300, 500, random));               // c_data
                            statement.addBatch();
                        }
                    }
                    statement.executeBatch();
                    Benchmark.updateStats("CUSTOMERS", configDistPerWhse * configCustPerDist, System.currentTimeMillis() - start);
                }
            }
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }

    public void loadCustomers() throws SQLException {
        try (Connection connection = makeConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE customer (\n" +
                        "  c_w_id int NOT NULL,\n" +
                        "  c_d_id int NOT NULL,\n" +
                        "  c_id int NOT NULL,\n" +
                        "  c_discount decimal(4,4) NOT NULL,\n" +
                        "  c_credit char(2) NOT NULL,\n" +
                        "  c_last varchar(16) NOT NULL,\n" +
                        "  c_first varchar(16) NOT NULL,\n" +
                        "  c_credit_lim decimal(12,2) NOT NULL,\n" +
                        "  c_balance decimal(12,2) NOT NULL,\n" +
                        "  c_ytd_payment float NOT NULL,\n" +
                        "  c_payment_cnt int NOT NULL,\n" +
                        "  c_delivery_cnt int NOT NULL,\n" +
                        "  c_street_1 varchar(20) NOT NULL,\n" +
                        "  c_street_2 varchar(20) NOT NULL,\n" +
                        "  c_city varchar(20) NOT NULL,\n" +
                        "  c_state char(2) NOT NULL,\n" +
                        "  c_zip char(9) NOT NULL,\n" +
                        "  c_phone char(16) NOT NULL,\n" +
                        "  c_since timestamp NOT NULL,\n" +
                        "  c_middle char(2) NOT NULL,\n" +
                        "  c_data varchar(500) NOT NULL,\n" +
                        "  PRIMARY KEY (c_w_id,c_d_id,c_id)\n" +
                        ")");
            }
        }

        taskId.set(0);
        Benchmark.runBenchmark(10, this::loadCustomer);
    }


    static class TPCCUtil {
        private static final String[] nameTokens = {"BAR", "OUGHT", "ABLE", "PRI",
                "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

        private static final int C_LAST_LOAD_C = 157; // in range [0, 255]

        private static String randomString(int length, char base, int numCharacters, Random r) {
            byte[] bytes = new byte[length];
            for (int i = 0; i < length; ++i) {
                bytes[i] = (byte) (base + r.nextInt(numCharacters));
            }
            return new String(bytes);
        }

        public static String randomStr(int minLen, int maxLen, Random r) {
            int strLen = minLen + r.nextInt(maxLen - minLen + 1);
            return strLen > 0 ? randomString(strLen, 'a', 26, r) : "";
        }

        public static String randomStr(int strLen, Random r) {
            return randomStr(strLen, strLen, r);
        }

        public static String randomNStr(int strLen, Random r) {
            return strLen > 0 ? randomString(strLen, '0', 10, r) : "";
        }

        public static int randomNumber(int min, int max, Random r) {
            return (int) (r.nextDouble() * (max - min + 1) + min);
        }

        public static int nonUniformRandom(int A, int C, int min, int max, Random r) {
            return (((randomNumber(0, A, r) | randomNumber(min, max, r)) + C) % (max - min + 1)) + min;
        }

        public static String getLastName(int num) {
            return nameTokens[num / 100] + nameTokens[(num / 10) % 10]
                    + nameTokens[num % 10];
        }

        public static String getNonUniformRandomLastNameForLoad(Random r) {
            return getLastName(nonUniformRandom(255, C_LAST_LOAD_C, 0, 999, r));
        }
    }
}
