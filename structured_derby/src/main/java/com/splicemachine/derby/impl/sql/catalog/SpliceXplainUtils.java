package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.hbase.SpliceDriver;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.jdbc.InternalDriver;

import java.sql.*;

/**
 * @author Scott Fines
 *         Date: 1/21/14
 */
public class SpliceXplainUtils {

    /**
     * Get the default or nested connection corresponding to the URL
     * jdbc:default:connection. We do not use DriverManager here
     * as it is not supported in JSR 169. IN addition we need to perform
     * more checks for null drivers or the driver returing null from connect
     * as that logic is in DriverManager.
     * @return The nested connection
     * @throws SQLException Not running in a SQL statement
     */
    public static Connection getDefaultConn()throws SQLException {
        InternalDriver id = InternalDriver.activeDriver();
        if (id != null) {
            Connection conn = id.connect("jdbc:default:connection", null);
            if (conn != null)
                return conn;
        }
        throw Util.noCurrentConnection();
    }

    public static String conglomerateToTableName(String conglomerateId) throws SQLException{

        String sql = "select s.schemaname, t.tablename from " +
                "sys.systables t, sys.sysschemas s,sys.sysconglomerates c " +
                "where " +
                "        t.schemaid = s.schemaid " +
                "        and t.tableid = c.tableid" +
                "        and c.conglomeratenumber = " + conglomerateId;

        Connection connection = SpliceDriver.driver().getInternalConnection();

        PreparedStatement s = connection.prepareStatement(sql);
        ResultSet resultSet = s.executeQuery();
        if (resultSet.next()) {
            return resultSet.getString(1) + "." + resultSet.getString(2);
        }

        return null;
    }
}
