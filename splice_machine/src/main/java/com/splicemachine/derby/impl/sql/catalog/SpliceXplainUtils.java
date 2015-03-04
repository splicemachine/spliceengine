package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.impl.jdbc.Util;
import com.splicemachine.db.jdbc.InternalDriver;

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
        String result = null;
        if (resultSet.next()) {
            result = resultSet.getString(1) + "." + resultSet.getString(2);
        }
        resultSet.close();
        return result;
    }
}
