package com.splicemachine.derby.impl.sql.catalog;

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

		private static boolean hasSchema(Connection conn, String schemaName) throws SQLException {
				ResultSet rs = conn.getMetaData().getSchemas();
				boolean schemaFound = false;
				while (rs.next() && !schemaFound)
						schemaFound = schemaName.equals(rs.getString("TABLE_SCHEM"));
				rs.close();
				return schemaFound;
		}

		private static boolean hasTable(Connection conn, String schemaName, String tableName) throws SQLException {
				ResultSet rs = conn.getMetaData().getTables(null, schemaName, tableName,  new String[] {"TABLE"});
				boolean tableFound = rs.next();
				rs.close();
				return tableFound;
		}

		private static void createXplainSchema(String schemaName) throws SQLException {
				Connection conn = getDefaultConn();
				if (!hasSchema(conn, schemaName)) {
						String escapedSchema = IdUtil.normalToDelimited(schemaName);
						Statement s = conn.createStatement();
						s.executeUpdate("CREATE SCHEMA " + escapedSchema);
						s.close();
						conn.commit();
				}
				conn.close();
		}



		private static void createXplainView(String schemaName,String viewName, String viewSQL) throws SQLException{
				Connection conn = getDefaultConn();
				if(!hasView(conn, schemaName, viewName)){
						Statement s = conn.createStatement();
						s.executeUpdate(viewSQL);
						s.close();
						conn.commit();
				}
				conn.close();
		}

		private static boolean hasView(Connection conn, String schemaName, String viewName) throws SQLException{
				ResultSet rs = conn.getMetaData().getTables(null, schemaName, viewName,  new String[] {"VIEW"});
				boolean tableFound = rs.next();
				rs.close();
				return tableFound;
		}

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


}
