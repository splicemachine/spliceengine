package org.apache.derby.testutils;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.dbutils.BasicRowProcessor;

/**
 * @author Jeff Cunningham
 *         Date: 6/25/14
 */
public interface ASTVisitorConfig {

    void setOutputStream(PrintStream out);

    void setConnection(Connection connection);

    void setStopAfterParse();

    void setContinueAfterParse();

    public static final class Factory {
        public static String EMBEDDED_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
        public static String CLIENT_DRIVER = "org.apache.derby.jdbc.ClientDriver";

        protected static String PROTOCOL = "jdbc:derby:";
        protected static String DB = "derbyDB";
        protected static String PORT = ":1527";
        protected static String CREATE = ";CREATE=true";
        protected static Properties PROPS = new Properties();

        private static String createProtocol(boolean createDB) {
            // port ignored for embedded connection
            return PROTOCOL + DB + PORT + (createDB ? CREATE : "");
        }

        public static Connection createConnection() throws Exception {
            loadDriver(CLIENT_DRIVER);
            Connection conn = null;
            try {
                conn = DriverManager.getConnection(createProtocol(true), PROPS);
            } catch (SQLException e) {
                if (e.getLocalizedMessage() != null && e.getLocalizedMessage().contains("Failed to start database")) {
                    // We get a SQLException from embedded driver when we don't have splice running
                    Exception e2 = new Exception("Splice server must be running.");
                    e2.initCause(e);
                    throw e2;
                }
                throw e;
            }
            return conn;
        }
        private static final String SHUTDOWN_URL = "jdbc:derby:spliceDB:1521;shutdown=true";
        public static void closeConnection(Connection connection) {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                // do nothing
            }
            try {
                DriverManager.getConnection(SHUTDOWN_URL);
            } catch (SQLException e) {
                // do nothing
            }
        }

        protected static synchronized void loadDriver(String driver) throws Exception {
            Class.forName(driver).newInstance();
        }

        public static void dropTable(Connection connection, String schemaName, String tableName) throws SQLException {
            Statement statement = connection.createStatement();
            statement.execute(String.format("drop table if exists %s.%s",schemaName,tableName));
            connection.commit();
        }

        public static int printResult(String statement, ResultSet rs, PrintStream out) throws SQLException {
            if (rs == null || rs.isClosed()) {
                return 0;
            }
            int resultSetSize = 0;
            out.println();
            out.println(statement);
            List<Map> maps = resultSetToMaps(rs);
            if (maps.size() > 0) {
                List<String> keys = new ArrayList<String>(maps.get(0).keySet());
                Collections.sort(keys);
                for (String col : keys) {
                    out.print(" "+col+" |");
                }
                out.println();
                for (int i=0; i<keys.size(); ++i) {
                    out.print("-----");
                }
                out.println();
                for (Map map : maps) {
                    ++resultSetSize;
                    for (String key : keys) {
                        out.print(" "+map.get(key)+" |");
                    }
                    out.println();
                }
            }
            out.println("--------------------");
            out.println(resultSetSize+" rows");
            return resultSetSize;
        }

        public static List<Map> resultSetToMaps(ResultSet rs) throws SQLException{

            List<Map> results = new ArrayList<Map>();
            BasicRowProcessor brp = new BasicRowProcessor();

            while(rs.next()){
                results.add(brp.toMap(rs));
            }

            return results;
        }

    }
}
