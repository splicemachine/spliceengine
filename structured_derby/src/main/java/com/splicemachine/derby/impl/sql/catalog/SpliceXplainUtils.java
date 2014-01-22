package com.splicemachine.derby.impl.sql.catalog;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.jdbc.InternalDriver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Scott Fines
 *         Date: 1/21/14
 */
public class SpliceXplainUtils {
		/**
		 * This procedure sets the current xplain schema.
		 * If the schema is not set, runtime statistics are captured as a
		 * textual stream printout. If it is set, statisitcs information is
		 * stored in that schema in user tables.
		 * @param schemaName May be an empty string.
		 * @throws java.sql.SQLException
		 */
		@SuppressWarnings("UnusedDeclaration")
		public static void SYSCS_SET_XPLAIN_SCHEMA(String schemaName)
																					throws SQLException, StandardException {
				//TODO -sf- deploy this out to all nodes in the cluster--JMX?
				LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();

				if (schemaName == null || schemaName.trim().length() == 0) {
						lcc.setXplainSchema(null);
						return;
				}

				boolean statsSave = lcc.getRunTimeStatisticsMode();
				lcc.setRunTimeStatisticsMode(false);
				createXplainSchema(schemaName);
				createXplainTable(schemaName,
								new XPLAINStatementHistoryDescriptor());
				createXplainTable(schemaName,
								new XPLAINOperationHistoryDescriptor());
				createXplainTable(schemaName,
								new XPLAINTaskDescriptor());
				lcc.setRunTimeStatisticsMode(statsSave);
				lcc.setXplainSchema(schemaName);
		}
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
				}
				conn.close();
		}

		// Create the XPLAIN table if it doesn't already exist
		private static void createXplainTable(String schemaName, XPLAINTaskDescriptor t) throws SQLException {
				String ddl = t.getTableDDL(schemaName);
				Connection conn = getDefaultConn();
				if (!hasTable(conn, schemaName, t.getTableName())) {
						Statement s = conn.createStatement();
						s.executeUpdate(ddl);
						s.close();
				}
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
